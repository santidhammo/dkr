package proxies

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/santidhammo/dkr/config"
	"github.com/santidhammo/dkr/messages"
	pb "github.com/santidhammo/dkr/remotesapi"
	"google.golang.org/grpc/metadata"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

/*
RequestProxy converts requests from a caller into the associated Kafka messages, and waits for Kafka responses.
*/
type RequestProxy struct {
	producer      *kafka.Producer
	chunkProducer *kafka.Producer
	pending       map[messages.MessageID]chan messages.Message
	closed        bool
	mutex         sync.Mutex
	config        *config.RequestProxyConfig
	pb.UnimplementedChunkStoreServiceServer
}

func NewRequestProxy(config *config.RequestProxyConfig) (*RequestProxy, error) {
	producer, err := kafka.NewProducer(config.ProducerKafkaConfig)
	if err != nil {
		return nil, err
	}
	chunkProducer, err := kafka.NewProducer(config.ProducerKafkaConfig)
	if err != nil {
		return nil, err
	}

	requestProxy := &RequestProxy{
		producer:      producer,
		chunkProducer: chunkProducer,
		pending:       make(map[messages.MessageID]chan messages.Message),
		closed:        false,
		config:        config,
	}

	return requestProxy, nil
}

func (p *RequestProxy) Start() {
	rpcConsumer := NewRPCConsumer(p.config.ConsumerKafkaConfig, p.config.ConsumingRPCTopic, func(m *messages.Message) {
		p.dispatchMessage(*m)
	}, func() bool {
		return p.closed
	})
	rpcConsumer.Start()
}

func (p *RequestProxy) GetRepoMetadata(ctx context.Context, in *pb.GetRepoMetadataRequest) (*pb.GetRepoMetadataResponse, error) {
	inMessage, err := messages.WrapGetRepoMetadataRequest(in)
	if err == nil {
		outMessage, err := p.produceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapGetRepoMetadataResponse(outMessage)
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) HasChunks(ctx context.Context, in *pb.HasChunksRequest) (*pb.HasChunksResponse, error) {
	inMessage, err := messages.WrapHasChunksRequest(in)
	if err == nil {
		outMessage, err := p.produceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapHasChunksResponse(outMessage)
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) GetDownloadLocations(ctx context.Context, in *pb.GetDownloadLocsRequest) (*pb.GetDownloadLocsResponse, error) {
	inMessage, err := messages.WrapGetDownloadLocsRequest(in)
	if err == nil {
		outMessage, err := p.produceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapGetDownloadLocsResponse(outMessage)
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) GetUploadLocations(ctx context.Context, in *pb.GetUploadLocsRequest) (*pb.GetUploadLocsResponse, error) {
	inMessage, err := messages.WrapGetUploadLocsRequest(in)
	if err == nil {
		outMessage, err := p.produceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapGetUploadLocsResponse(outMessage)
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) Rebase(ctx context.Context, in *pb.RebaseRequest) (*pb.RebaseResponse, error) {
	inMessage, err := messages.WrapRebaseRequest(in)
	if err == nil {
		outMessage, err := p.produceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapRebaseResponse(outMessage)
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) Root(ctx context.Context, in *pb.RootRequest) (*pb.RootResponse, error) {
	inMessage, err := messages.WrapRootRequest(in)
	if err == nil {
		outMessage, err := p.produceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapRootResponse(outMessage)
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) Commit(ctx context.Context, in *pb.CommitRequest) (*pb.CommitResponse, error) {
	inMessage, err := messages.WrapCommitRequest(in)
	if err == nil {
		outMessage, err := p.produceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapCommitResponse(outMessage)
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) ListTableFiles(ctx context.Context, in *pb.ListTableFilesRequest) (*pb.ListTableFilesResponse, error) {
	inMessage, err := messages.WrapListTableFilesRequest(in)
	if err == nil {
		outMessage, err := p.produceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapListTableFilesResponse(outMessage)
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) RefreshTableFileUrl(ctx context.Context, in *pb.RefreshTableFileUrlRequest) (*pb.RefreshTableFileUrlResponse, error) {
	inMessage, err := messages.WrapRefreshTableFileUrlRequest(in)
	if err == nil {
		outMessage, err := p.produceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapRefreshTableFileUrlResponse(outMessage)
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) AddTableFiles(ctx context.Context, in *pb.AddTableFilesRequest) (*pb.AddTableFilesResponse, error) {
	inMessage, err := messages.WrapAddTableFilesRequest(in)
	if err == nil {
		outMessage, err := p.produceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapAddTableFilesResponse(outMessage)
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) StreamDownloadLocations(stream pb.ChunkStoreService_StreamDownloadLocationsServer) (err error) {
	ctx := stream.Context()
	id := messages.NewMessageID()
	p.createMessageChannelIfNotExists(id)

	errs := make(chan error)
	var wg sync.WaitGroup
	wg.Add(1)
	defer func() {
		wg.Wait()
		err = <-errs
	}()

	go p.streamDownloadLocationsSender(id, stream, &wg, errs)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var req *pb.GetDownloadLocsRequest
		req, err = stream.Recv()
		if err == io.EOF {
			message := messages.NewStreamingMessageEndTransmission(messages.StreamDownloadLocations, id)
			err = p.produceAndVerify(message)
			if err != nil {
				return
			}
		}

		if err != nil {
			return
		}

		var message *messages.Message
		message, err = messages.WrapStreamingDownloadLocsRequest(req, id)
		if err != nil {
			return
		}

		err = p.produceAndVerify(message)
		if err != nil {
			return
		}
	}
}

func (p *RequestProxy) streamDownloadLocationsSender(id messages.MessageID, stream pb.ChunkStoreService_StreamDownloadLocationsServer, wg *sync.WaitGroup, errs chan error) {
	defer func() {
		wg.Done()
		close(errs)
	}()
	channel := p.pending[id]
	if channel != nil {
		for {
			log.Println("Awaiting next message for Message ID:", id.String())
			message := <-channel
			if message.EndOfTx {
				return
			}
			response, err := messages.UnWrapStreamingDownloadLocsResponse(&message)
			if err != nil {
				errs <- err
				return
			}

			shouldReturn := p.replaceDownloadChunkUrls(stream, response, errs)
			if shouldReturn {
				return
			}

			err = stream.Send(response)
			if err != nil {
				errs <- err
				return
			}
		}
	}
}

func (p *RequestProxy) replaceDownloadChunkUrls(stream pb.ChunkStoreService_StreamDownloadLocationsServer, response *pb.GetDownloadLocsResponse, errs chan error) bool {
	md, _ := metadata.FromIncomingContext(stream.Context())

	for _, loc := range response.Locs {
		oldGet := loc.GetHttpGet()
		if oldGet != nil {
			u, err := url.Parse(oldGet.Url)
			if err != nil {
				errs <- err
				return true
			}

			replacedUrl := p.getDownloadUrl(md, u.Path, u.RawQuery)
			log.Println("Replaced url:", u, "with:", replacedUrl)

			newGet := pb.HttpGetChunk{}
			newGet.Hashes = oldGet.Hashes
			newGet.Url = replacedUrl.String()

			loc.Location = &pb.DownloadLoc_HttpGet{HttpGet: &newGet}

		}

		oldGetRange := loc.GetHttpGetRange()
		if oldGetRange != nil {
			u, err := url.Parse(oldGetRange.Url)
			if err != nil {
				errs <- err
				return true
			}
			replacedUrl := p.getDownloadUrl(md, u.Path, u.RawQuery)
			log.Println("Replaced url:", u, "with:", replacedUrl)

			newGetRange := pb.HttpGetRange{}
			newGetRange.Ranges = oldGetRange.Ranges
			newGetRange.Url = replacedUrl.String()

			loc.Location = &pb.DownloadLoc_HttpGetRange{HttpGetRange: &newGetRange}
		}
	}
	return false
}

func (p *RequestProxy) createMessageChannelIfNotExists(id messages.MessageID) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	_, ok := p.pending[id]
	if !ok {
		log.Println("Create message channel for Message ID:", id.String())
		p.pending[id] = make(chan messages.Message)
	}
}

func (p *RequestProxy) dispatchMessage(message messages.Message) {
	channel := p.pending[message.ID]
	if channel != nil {
		log.Println("Dispatching message:", message.String())
		select {
		case channel <- message:
			log.Println("Dispatched message:", message.String())
		case <-time.After(time.Second):
			log.Println("Dispatching message:", message.String(), "failed, received timeout")
		}
		if message.EndOfTx {
			delete(p.pending, message.ID)
		}
	} else {
		log.Println("Ignore message:", message.String())
	}
}

func (p *RequestProxy) produceAndConsumeMessage(message *messages.Message) (*messages.Message, error) {
	if message != nil {
		producerChannel, err := p.produceMessage(*message)
		if err != nil {
			return nil, err
		}
		for {
			e := <-producerChannel
			switch ev := e.(type) {
			case *kafka.Message:
				return p.awaitConsumption(ev, message.ID)
			}
		}
	} else {
		return nil, fmt.Errorf("no message to produce given")
	}
}

func (p *RequestProxy) produceMessage(message messages.Message) (chan kafka.Event, error) {
	encoded, err := message.Encode()
	if err != nil {
		return nil, err
	}
	kafkaMessage := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.config.ProducingRPCTopic, Partition: kafka.PartitionAny},
		Value:          encoded,
	}
	p.createMessageChannelIfNotExists(message.ID)
	producerChannel := make(chan kafka.Event)
	log.Println("Producing message:", message.String())
	err = p.producer.Produce(&kafkaMessage, producerChannel)
	if err != nil {
		return nil, err
	}
	return producerChannel, nil
}

func (p *RequestProxy) awaitConsumption(ev *kafka.Message, id messages.MessageID) (*messages.Message, error) {
	log.Println("Awaiting consumption of message:", id.String())
	if ev.TopicPartition.Error != nil {
		delete(p.pending, id)
		return nil, ev.TopicPartition.Error
	} else {
		channel := p.pending[id]
		select {
		case decodedMessage := <-channel:
			return &decodedMessage, nil
		case <-time.After(time.Second * 5):
			delete(p.pending, id)
			// TODO: should create proper error
			return nil, errors.New("did not receive response after request/response timeout for message")
		}
	}
}

func (p *RequestProxy) produceAndVerify(message *messages.Message) error {
	producerChannel, err := p.produceMessage(*message)
	if err != nil {
		return err
	}

	for {
		e := <-producerChannel
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				delete(p.pending, message.ID)
				return ev.TopicPartition.Error
			}
			return nil
		}
	}
}

func (p *RequestProxy) getDownloadUrl(md metadata.MD, path string, query string) *url.URL {
	host := p.getHost(md)
	return &url.URL{
		Scheme:   p.config.HttpScheme(),
		Host:     host,
		Path:     path,
		RawQuery: query,
	}
}

func (p *RequestProxy) getHost(md metadata.MD) string {
	defaultPort := strconv.Itoa(int(p.config.HttpPort))
	host := []string{"", defaultPort}
	authorityHosts := md.Get(":authority")

	if len(p.config.ExternalHttpHost) > 0 {
		host[0] = p.config.ExternalHttpHost
	} else if len(authorityHosts) > 0 {
		host[0] = strings.Split(authorityHosts[0], ":")[0]
	}

	if p.config.ExternalHttpPort > 0 {
		host[1] = strconv.Itoa(int(p.config.ExternalHttpPort))
	}

	return strings.Join(host, ":")
}

func (p *RequestProxy) Close() {
	p.closed = true
}
