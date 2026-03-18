package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	readUDP  string
	writeUDP string
	readTCP  string
	writeTCP string

	limit int

	d  = net.Dialer{Timeout: 5 * time.Second}
	lc = net.ListenConfig{KeepAliveConfig: net.KeepAliveConfig{Enable: true}}
)

const maxDatagramSize = 65535

func main() {
	flag.StringVar(&readUDP, "readudp", "", "Read from UDP address")
	flag.StringVar(&writeUDP, "writeudp", "", "Write to UDP address")
	flag.StringVar(&readTCP, "readtcp", "", "Read from TCP address")
	flag.StringVar(&writeTCP, "writetcp", "", "Write to TCP address")
	flag.IntVar(&limit, "limit", 1000, "Limit number of connections")

	flag.Parse()

	ctx, stopNotify := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopNotify()

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(limit)

	if readUDP != "" && writeUDP != "" {
		group.Go(func() error {
			return udpAcceptLoop(ctx, group, writeUDP, readUDP)
		})
	}

	if readTCP != "" && writeTCP != "" {
		group.Go(func() error {
			return tcpAcceptLoop(ctx, group, writeTCP, readTCP)
		})
	}

	err := group.Wait()
	if err != nil {
		log.Printf("%s, exit", err)
	}
}

func newUDPProxy(ctx context.Context, group *errgroup.Group, listener net.PacketConn, upstreamAddr string) *udpProxy {
	proxy := &udpProxy{
		group:        group,
		listener:     listener,
		upstreamAddr: upstreamAddr,
		sessions:     make(map[string]*udpSession),
		messages:     make(chan message),
	}

	group.Go(func() error {
		return proxy.router(ctx)
	})

	group.Go(func() error {
		ticker := time.NewTicker(10 * time.Second)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				proxy.gc()
			}
		}
	})

	return proxy
}

type udpProxy struct {
	group        *errgroup.Group
	listener     net.PacketConn
	upstreamAddr string

	sessionMutex sync.RWMutex
	sessions     map[string]*udpSession

	messages chan message
}

func (p *udpProxy) router(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-p.messages:
			session, ok := p.getSession(msg.addr.String())
			if !ok {
				var err error
				session, err = p.newSession(ctx, msg.addr)
				if err != nil {
					log.Printf("new session %s: %s", msg.addr.String(), err)
					continue
				}
			} else {
				session.lastUsed.Store(new(time.Now()))
			}

			session.messages <- msg.buf
		}
	}
}

func (p *udpProxy) getSession(addr string) (*udpSession, bool) {
	p.sessionMutex.RLock()
	defer p.sessionMutex.RUnlock()

	session, ok := p.sessions[addr]
	return session, ok
}

func (p *udpProxy) newSession(ctx context.Context, addr net.Addr) (*udpSession, error) {
	upstream, err := d.DialContext(ctx, "udp", p.upstreamAddr)
	if err != nil {
		return nil, fmt.Errorf("dial %s udp: %s", p.upstreamAddr, err)
	}

	session := &udpSession{
		addr:         addr,
		upstreamConn: upstream,
		messages:     make(chan []byte),
	}

	group, ctx := errgroup.WithContext(ctx)
	session.ctx, session.cancel = context.WithCancel(ctx)
	session.lastUsed.Store(new(time.Now()))

	p.group.Go(func() error {
		defer upstream.Close()

		group.Go(func() error {
			return p.read(session)
		})

		group.Go(func() error {
			return p.write(session)
		})

		err := group.Wait()
		if err != nil {
			log.Printf("session %s: %s", addr, err)
		}

		return nil
	})

	p.sessionMutex.Lock()
	_, ok := p.sessions[addr.String()]
	if ok {
		panic("session already exists")
	}

	p.sessions[addr.String()] = session
	p.sessionMutex.Unlock()

	return session, nil
}

func (p *udpProxy) read(session *udpSession) error {
	for {
		select {
		case <-session.ctx.Done():
			return session.ctx.Err()
		case buf := <-session.messages:
			n, err := session.upstreamConn.Write(buf)
			if n != len(buf) {
				panic("short write")
			}

			if err != nil {
				return fmt.Errorf("write from %s: %w", session.addr.String(), err)
			}
		}
	}
}

func (p *udpProxy) write(session *udpSession) (err error) {
	buf := make([]byte, maxDatagramSize)
	for {
		nr, er := session.upstreamConn.Read(buf)
		if nr > 0 {
			nw, ew := p.listener.WriteTo(buf[0:nr], session.addr)
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write")
				}
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}

	return err
}

func (p *udpProxy) gc() {
	p.sessionMutex.Lock()
	defer p.sessionMutex.Unlock()

	for addr, session := range p.sessions {
		if time.Now().Before(session.lastUsed.Load().Add(time.Minute)) {
			continue
		}

		session.cancel()
		delete(p.sessions, addr)
	}
}

type udpSession struct {
	ctx          context.Context
	cancel       context.CancelFunc
	addr         net.Addr
	upstreamConn net.Conn
	lastUsed     atomic.Pointer[time.Time]
	messages     chan []byte
}

type message struct {
	addr net.Addr
	buf  []byte
}

func udpAcceptLoop(ctx context.Context, group *errgroup.Group, writeAddr, readAddr string) error {
	reader, err := lc.ListenPacket(ctx, "udp", readAddr)
	if err != nil {
		return fmt.Errorf("listen %s udp: %w", readAddr, err)
	}
	defer reader.Close()

	proxy := newUDPProxy(ctx, group, reader, writeAddr)
	for {
		err = ctx.Err()
		if err != nil {
			return err
		}

		//TODO: buf pool
		buf := make([]byte, maxDatagramSize)
		n, addr, err := reader.ReadFrom(buf)
		if err != nil {
			log.Printf("Read from udp listener from %s: %s", addr, err)
		}

		proxy.messages <- message{
			addr: addr,
			buf:  buf[:n],
		}
	}
}

func tcpAcceptLoop(ctx context.Context, group *errgroup.Group, writeAddr, readAddr string) error {
	reader, err := lc.Listen(ctx, "tcp", readAddr)
	if err != nil {
		return fmt.Errorf("listen %s tcp: %w", readAddr, err)
	}
	defer reader.Close()

	for {
		err = ctx.Err()
		if err != nil {
			return err
		}

		conn, err := reader.Accept()
		if err != nil {
			return fmt.Errorf("accept tcp: %w", err)
		}

		group.Go(func() error {
			defer conn.Close()

			upstream, err := d.DialContext(ctx, "tcp", writeAddr)
			if err != nil {
				log.Printf("dial %s tcp: %s", writeAddr, err)
				return nil
			}
			defer upstream.Close()

			var connGroup errgroup.Group
			connGroup.Go(func() error {
				_, err = io.Copy(upstream, conn)
				return err
			})

			connGroup.Go(func() error {
				_, err = io.Copy(conn, upstream)
				return err
			})

			err = connGroup.Wait()
			if err != nil {
				log.Printf("copy %s to %s tcp: %s", readAddr, writeAddr, err)
			}

			return nil
		})
	}
}
