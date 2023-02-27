package messages

import (
	"bytes"
	"encoding/gob"

	"github.com/google/uuid"
)

type PayloadType uint8

func (t *PayloadType) String() string {
	switch *t {
	case GetRepoMetadata:
		return "GetRepoMetadata"
	case HasChunks:
		return "HasChunks"
	case GetDownloadLocations:
		return "GetDownloadLocations"
	case StreamDownloadLocations:
		return "StreamDownloadLocations"
	case GetUploadLocations:
		return "GetUploadLocations"
	case Rebase:
		return "Rebase"
	case Root:
		return "Root"
	case Commit:
		return "Commit"
	case ListTableFiles:
		return "ListTableFiles"
	case RefreshTableFileUrl:
		return "RefreshTableFileUrl"
	case AddTableFiles:
		return "AddTableFiles"
	default:
		return "(No Payload/Unknown Payload Type)"
	}
}

const (
	GetRepoMetadata         = 0
	HasChunks               = 1
	GetDownloadLocations    = 2
	StreamDownloadLocations = 3
	GetUploadLocations      = 4
	Rebase                  = 5
	Root                    = 6
	Commit                  = 7
	ListTableFiles          = 8
	RefreshTableFileUrl     = 9
	AddTableFiles           = 10
)

type MessageID uuid.UUID

func (m *MessageID) String() string {
	return uuid.UUID(*m).String()
}

type Message struct {
	// The Payload type indicates what is to be expected for kind of message
	PayloadType PayloadType

	// If receiving this message, this message is the last in a row of
	// adjacent requests/responses
	EndOfTx bool

	// The Payload contains the actual message
	Payload []byte

	// Since requests and responses are send across two topics, they do not
	// need to be in order. Therefore, it is important to reconstruct the
	// order by tracking the original request and match it with a response when
	// it has arrived.
	ID MessageID
}

func (m *Message) String() string {
	return "ID: " + m.ID.String() + " Payload Type: " + m.PayloadType.String()
}

func (m *Message) Encode() ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(m)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func Decode(encoded []byte) (*Message, error) {
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	var m Message
	err := decoder.Decode(&m)
	if err != nil {
		return nil, err
	} else {
		return &m, nil
	}
}

func NewTrackedMessage(payloadType PayloadType, payload []byte) *Message {
	return &Message{
		PayloadType: payloadType,
		EndOfTx:     true,
		Payload:     payload,
		ID:          NewMessageID(),
	}
}

func NewStreamingMessage(payloadType PayloadType, payload []byte, id MessageID) *Message {
	return &Message{
		PayloadType: payloadType,
		EndOfTx:     false,
		Payload:     payload,
		ID:          id,
	}
}

func NewStreamingMessageEndTransmission(payloadType PayloadType, id MessageID) *Message {
	return &Message{
		PayloadType: payloadType,
		EndOfTx:     true,
		ID:          id,
	}
}

func NewResponseMessage(payloadType PayloadType, payload []byte, id MessageID) *Message {
	return &Message{
		PayloadType: payloadType,
		EndOfTx:     true,
		Payload:     payload,
		ID:          id,
	}
}

func NewMessageID() MessageID {
	return MessageID(uuid.New())
}
