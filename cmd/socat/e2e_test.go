package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/libmonsoon-dev/socat/cmd/internal/controller"
	"golang.org/x/sync/errgroup"
)

const (
	expectedRequest  = "Hello from client"
	expectedResponse = "Hello from server"
)

func TestReverseE2E(t *testing.T) {
	tServer := httptest.NewServer(testHandler(t))
	defer tServer.Close()

	tServerAddr, err := url.Parse(tServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	const (
		authToken      = "token"
		masterPort     = 1337
		controllerPort = 1338
	)

	ctx, cancel := context.WithCancel(t.Context())
	group, ctx := errgroup.WithContext(ctx)

	masterConf := Config{
		Mode:       ReverseMasterMode,
		ReadTCP:    localHostAddr(masterPort),
		WriteTCP:   tServerAddr.Host,
		Controller: controller.Config{Address: localHostAddr(controllerPort), AuthToken: authToken},
	}
	New(masterConf).Run(ctx, group)

	slaveConf := Config{
		Mode:       ReverseSlaveMode,
		Controller: controller.Config{Address: localHostAddr(controllerPort), AuthToken: authToken},
	}
	New(slaveConf).Run(ctx, group)

	resp, err := tServer.
		Client().
		Post(
			(&url.URL{Scheme: "http", Host: localHostAddr(masterPort)}).String(),
			"text/plain",
			strings.NewReader(expectedRequest),
		)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if string(respBody) != expectedResponse {
		t.Fatalf("got response body %q, want %q", respBody, expectedResponse)
	}

	cancel()
	_ = group.Wait()
}

func localHostAddr(port int) string {
	return net.JoinHostPort("localhost", strconv.Itoa(port))
}

func testHandler(t *testing.T) http.HandlerFunc {
	t.Helper()

	return func(w http.ResponseWriter, r *http.Request) {
		reqBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}

		if string(reqBody) != expectedRequest {
			t.Fatalf("got request body %q, want %q", reqBody, expectedRequest)
		}

		_, err = w.Write([]byte(expectedResponse))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}

	return val
}
