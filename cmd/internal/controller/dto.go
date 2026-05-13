package controller

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

type messageType int

const (
	newConnectionRequestType messageType = iota
	newConnectionResponseType
)

type message struct {
	CorrelationID uuid.UUID
	Type          messageType
	Data          json.RawMessage
}

func newRequest(data any) message {
	return newResponse(uuid.Must(uuid.NewV7()), data)
}

func newResponse(correlationID uuid.UUID, data any) message {
	rawMessage, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	return message{
		CorrelationID: correlationID,
		Type:          getControllerMessageType(data),
		Data:          rawMessage,
	}
}

func getControllerMessageType(data any) messageType {
	switch data.(type) {
	case newConnectionRequest:
		return newConnectionRequestType
	default:
		panic(fmt.Sprintf("unexpected data type %T", data))
	}
}

type newConnectionRequest struct {
	ID      uuid.UUID
	WriteTo string
}
