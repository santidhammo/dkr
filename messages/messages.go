package messages

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

type RPCPayloadType uint8

func (t *RPCPayloadType) String() string {
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
		return "(No Body/Unknown Body Type)"
	}
}

const (
	GetRepoMetadata RPCPayloadType = iota
	HasChunks
	GetDownloadLocations
	StreamDownloadLocations
	GetUploadLocations
	Rebase
	Root
	Commit
	ListTableFiles
	RefreshTableFileUrl
	AddTableFiles
)

type ChunkPayloadType uint8

func (t *ChunkPayloadType) String() string {
	switch *t {
	case RequestDownload:
		return "RequestDownload"
	case RequestUpload:
		return "RequestUpload"
	default:
		return "(No Body/Unknown Body Type)"
	}
}

const (
	RequestDownload ChunkPayloadType = iota
	RequestUpload
)

type MessageType uint8

const (
	RPC MessageType = iota
	Chunk
)

type MessageID uuid.UUID

func NewMessageID() MessageID {
	return MessageID(uuid.New())
}

func (m *MessageID) String() string {
	return uuid.UUID(*m).String()
}

type Message interface {
	fmt.Stringer
	Identifier() MessageID
	Type() MessageType
	Encode() ([]byte, error)
	EndOfTransaction() bool
}

type RPCMessage struct {
	// The Payload type indicates what is to be expected for kind of message
	PayloadType RPCPayloadType

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

func (m *RPCMessage) Identifier() MessageID {
	return m.ID
}

func (m *RPCMessage) Type() MessageType {
	return RPC
}

func (m *RPCMessage) Encode() ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	buffer.WriteByte(byte(m.Type()))
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(m)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (m *RPCMessage) EndOfTransaction() bool {
	return m.EndOfTx
}

func (m *RPCMessage) String() string {
	eot := "false"
	if m.EndOfTx {
		eot = "true"
	}
	return "RPC Message, ID: " + m.ID.String() + " Body Type: " + m.PayloadType.String() + " EOT? " + eot
}

func Decode(encoded []byte) (Message, error) {
	buffer := bytes.NewBuffer(encoded)
	firstByte, err := buffer.ReadByte()
	if err != nil {
		return nil, err
	}
	decoder := gob.NewDecoder(buffer)
	switch MessageType(firstByte) {
	case RPC:
		var m RPCMessage
		err := decoder.Decode(&m)
		if err != nil {
			return nil, err
		} else {
			return &m, nil
		}
	case Chunk:
		var m ChunkMessage
		err := decoder.Decode(&m)
		if err != nil {
			return nil, err
		} else {
			return &m, nil
		}
	}

	return nil, errors.New("could not decode message as it did not contain the first byte of the message type")
}

func NewTrackedRPCMessage(payloadType RPCPayloadType, payload []byte) *RPCMessage {
	return &RPCMessage{
		PayloadType: payloadType,
		EndOfTx:     true,
		Payload:     payload,
		ID:          NewMessageID(),
	}
}

func NewStreamingRPCMessage(payloadType RPCPayloadType, payload []byte, id MessageID) *RPCMessage {
	return &RPCMessage{
		PayloadType: payloadType,
		EndOfTx:     false,
		Payload:     payload,
		ID:          id,
	}
}

func NewStreamingRPCMessageEndTransmission(payloadType RPCPayloadType, id MessageID) *RPCMessage {
	return &RPCMessage{
		PayloadType: payloadType,
		EndOfTx:     true,
		ID:          id,
	}
}

func NewResponseRPCMessage(payloadType RPCPayloadType, payload []byte, id MessageID) *RPCMessage {
	return &RPCMessage{
		PayloadType: payloadType,
		EndOfTx:     true,
		Payload:     payload,
		ID:          id,
	}
}

type ChunkMessage struct {
	// The Body type indicates what is to be expected for kind of message
	PayloadType ChunkPayloadType

	// If receiving this message, this message is the last in a row of
	// adjacent requests/responses
	EndOfTx bool

	// The Body contains the body of the request or response
	Body []byte

	// The URL contains the URL of the request or response, as a string
	URL string

	// Additional necessary Headers are send alongside the message and passed through
	Headers map[string]string

	// Since requests and responses are send across two topics, they do not
	// need to be in order. Therefore, it is important to reconstruct the
	// order by tracking the original request and match it with a response when
	// it has arrived.
	ID MessageID
}

func NewTrackedChunkMessage(
	payloadType ChunkPayloadType,
	body []byte,
	url string,
	headers map[string]string) *ChunkMessage {
	return &ChunkMessage{
		PayloadType: payloadType,
		EndOfTx:     true,
		Body:        body,
		URL:         url,
		Headers:     headers,
		ID:          NewMessageID(),
	}
}

func NewResponseChunkMessage(
	payloadType ChunkPayloadType,
	body []byte,
	headers map[string]string,
	id MessageID) *ChunkMessage {
	return &ChunkMessage{
		PayloadType: payloadType,
		EndOfTx:     true,
		Body:        body,
		URL:         "",
		Headers:     headers,
		ID:          id,
	}
}

func (m *ChunkMessage) Identifier() MessageID {
	return m.ID
}

func (m *ChunkMessage) Type() MessageType {
	return Chunk
}

func (m *ChunkMessage) Encode() ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	buffer.WriteByte(byte(m.Type()))
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(m)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (m *ChunkMessage) EndOfTransaction() bool {
	return m.EndOfTx
}

func (m *ChunkMessage) String() string {
	eot := "false"
	if m.EndOfTx {
		eot = "true"
	}
	return "Chunk Message, ID: " + m.ID.String() + " Body Type: " + m.PayloadType.String() + " EOT? " + eot
}
