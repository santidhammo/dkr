package proxies

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/santidhammo/dkr/config"
	"github.com/santidhammo/dkr/messages"
	"github.com/santidhammo/dkr/remotesapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"sync"
)

type ResponseProxy struct {
	producer                   *kafka.Producer
	chunkProducer              *kafka.Producer
	config                     *config.ResponseProxyConfig
	client                     remotesapi.ChunkStoreServiceClient
	context                    context.Context
	closed                     bool
	downloadLocationsClientExs map[messages.MessageID]StreamDownloadLocationsClientEx
}

type StreamDownloadLocationsClientEx struct {
	client remotesapi.ChunkStoreService_StreamDownloadLocationsClient
	wg     *sync.WaitGroup
}

func NewResponseProxy(config *config.ResponseProxyConfig) (*ResponseProxy, error) {
	conn, err := grpc.Dial(config.RemoteURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := remotesapi.NewChunkStoreServiceClient(conn)

	producer, err := kafka.NewProducer(config.ProducerKafkaConfig)
	if err != nil {
		return nil, err
	}
	//chunkConsumer, err := kafka.NewConsumer(config.ConsumerKafkaConfig)
	//if err != nil {
	//	return nil, err
	//}
	chunkProducer, err := kafka.NewProducer(config.ProducerKafkaConfig)
	if err != nil {
		return nil, err
	}

	responseProxy := &ResponseProxy{
		producer:                   producer,
		chunkProducer:              chunkProducer,
		config:                     config,
		client:                     client,
		context:                    context.Background(),
		closed:                     false,
		downloadLocationsClientExs: make(map[messages.MessageID]StreamDownloadLocationsClientEx),
	}

	return responseProxy, nil
}

func (p *ResponseProxy) Start() {
	rpcConsumer := NewRPCConsumer(p.config.ConsumerKafkaConfig, p.config.ConsumingRPCTopic, func(m *messages.Message) {
		p.handleMessage(m)
	}, func() bool {
		return p.closed
	})
	rpcConsumer.Start()
}

func (p *ResponseProxy) handleMessage(message *messages.Message) {
	if message != nil {
		log.Println("Processing message:", message.String())
		switch message.PayloadType {
		case messages.GetRepoMetadata:
			p.handleGetRepoMetadataRequest(message)
		case messages.Root:
			p.handleRootRequest(message)
		case messages.StreamDownloadLocations:
			p.handleStreamDownloadLocations(message)
		}
	}
}

func (p *ResponseProxy) handleGetRepoMetadataRequest(in *messages.Message) {
	request, err := messages.UnWrapGetRepoMetadataRequest(in)
	if err != nil {
		log.Println("Could not unwrap GetRepoMetadataRequest from consumed message")
	} else {
		response, err := p.client.GetRepoMetadata(p.context, request)
		if err != nil {
			log.Println("Could not request gRPC:", err.Error())
		} else {
			out, err := messages.WrapGetRepoMetadataResponse(response, in.ID)
			p.produceWithErrorLogging(err, out)
		}
	}
}

func (p *ResponseProxy) handleRootRequest(in *messages.Message) {
	request, err := messages.UnWrapRootRequest(in)
	if err != nil {
		log.Println("Could not unwrap RootRequest from consumed message")
	} else {
		response, err := p.client.Root(p.context, request)
		if err != nil {
			log.Println("Could not request gRPC:", err.Error())
		} else {
			out, err := messages.WrapRootResponse(response, in.ID)
			p.produceWithErrorLogging(err, out)
		}
	}
}

func (p *ResponseProxy) handleStreamDownloadLocations(in *messages.Message) {
	if in.EndOfTx {
		log.Println("Deleting streaming client for:", in.ID.String())
		clientEx, clientRegistered := p.downloadLocationsClientExs[in.ID]
		if clientRegistered {
			log.Println("Waiting until production on client for:", in.ID.String(), "stopped")
			clientEx.wg.Wait()
			delete(p.downloadLocationsClientExs, in.ID)
		}
		return
	}
	request, err := messages.UnWrapStreamingDownloadLocsRequest(in)
	if err != nil {
		log.Println("Could not unwrap RootRequest from consumed message")
	} else {
		clientEx, clientRegistered := p.downloadLocationsClientExs[in.ID]
		if clientRegistered {
			err := clientEx.client.Send(request)
			if err != nil {
				log.Println("Could not request gRPC:", err.Error())
			}
		} else {
			client, err := p.client.StreamDownloadLocations(p.context)
			if err != nil {
				log.Println("Could not request gRPC:", err.Error())
			} else {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				clientEx := StreamDownloadLocationsClientEx{
					client,
					wg,
				}
				p.downloadLocationsClientExs[in.ID] = clientEx
				go p.proxyStreamDownloadLocationsResponses(in, clientEx)
				err := client.Send(request)
				if err != nil {
					log.Println("Could not request gRPC:", err.Error())
				}
			}
		}

	}
}

func (p *ResponseProxy) proxyStreamDownloadLocationsResponses(
	in *messages.Message,
	clientEx StreamDownloadLocationsClientEx,
) {

	for {
		response, err := clientEx.client.Recv()
		log.Println("Received new streaming response for:", in.ID)
		if err != nil {
			if err == io.EOF {
				log.Println("No more responses on streaming download locations for", in.ID)
				out := messages.NewStreamingMessageEndTransmission(messages.StreamDownloadLocations, in.ID)
				err := p.produceMessage(*out)
				if err != nil {
					log.Println("Received error while sending EOT for", in.ID)
				}
			} else {
				log.Println("Received error while receiving download locations for", in.ID)
			}
			clientEx.wg.Done()
			return
		}
		out, err := messages.WrapStreamingDownloadLocsResponse(response, in.ID)
		log.Println(response.Locs)
		p.produceWithErrorLogging(err, out)
	}
}

func (p *ResponseProxy) produceWithErrorLogging(err error, out *messages.Message) {
	if err != nil {
		log.Println("Received error while wrapping:", err.Error())
	} else {
		err := p.produceMessage(*out)
		if err != nil {
			log.Println("Received error while sending response back to the request proxy:", err.Error())
		}
	}
}

func (p *ResponseProxy) produceMessage(message messages.Message) error {
	encoded, err := message.Encode()
	if err != nil {
		return nil
	}
	kafkaMessage := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.config.ProducingRPCTopic, Partition: kafka.PartitionAny},
		Value:          encoded,
	}
	producerChannel := make(chan kafka.Event)
	log.Println("Producing message:", message.String())
	err = p.producer.Produce(&kafkaMessage, producerChannel)
	if err != nil {
		return nil
	}

	err = p.awaitMessageVerification(producerChannel)
	if err != nil {
		return err
	}
	return nil
}

func (p *ResponseProxy) awaitMessageVerification(producerChannel chan kafka.Event) error {
	for {
		e := <-producerChannel
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				return ev.TopicPartition.Error
			}
			return nil
		}
	}
}

func (p *ResponseProxy) Close() {
	p.closed = true
}
