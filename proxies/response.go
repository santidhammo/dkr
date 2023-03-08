package proxies

import (
	"bytes"
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/santidhammo/dkr/config"
	"github.com/santidhammo/dkr/messages"
	"github.com/santidhammo/dkr/remotesapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"net/http"
	"sync"
)

type ResponseProxy struct {
	producer                   *kafka.Producer
	chunkProducer              *kafka.Producer
	config                     *config.ResponseProxyConfig
	client                     remotesapi.ChunkStoreServiceClient
	context                    context.Context
	downloadLocationsClientExs map[messages.MessageID]StreamDownloadLocationsClientEx
	rpcConsumer                *MessageConsumer
	isStarted                  bool
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
		downloadLocationsClientExs: make(map[messages.MessageID]StreamDownloadLocationsClientEx),
		rpcConsumer:                nil,
		isStarted:                  false,
	}

	responseProxy.rpcConsumer = NewMessageConsumer(
		config.ConsumerKafkaConfig,
		config.ConsumingTopic,
		func(m messages.Message) {
			responseProxy.handleMessage(m)
		})

	return responseProxy, nil
}

func (p *ResponseProxy) Start() {
	if !p.isStarted {
		go p.rpcConsumer.Start()
		p.isStarted = true
	}
}

func (p *ResponseProxy) handleMessage(message messages.Message) {
	if message != nil {
		log.Println("Processing message:", message.String())
		switch message.(type) {
		case *messages.RPCMessage:
			rpcMessage := message.(*messages.RPCMessage)
			switch rpcMessage.PayloadType {
			case messages.GetRepoMetadata:
				p.handleGetRepoMetadataRequest(rpcMessage)
			case messages.Root:
				p.handleRootRequest(rpcMessage)
			case messages.StreamDownloadLocations:
				p.handleStreamDownloadLocations(rpcMessage)
			case messages.HasChunks:
				p.handleHasChunks(rpcMessage)
			case messages.ListTableFiles:
				p.handleListTableFiles(rpcMessage)
			case messages.GetUploadLocations:
				p.handleGetUploadLocations(rpcMessage)
			case messages.AddTableFiles:
				p.handleAddTableFiles(rpcMessage)
			case messages.Commit:
				p.handleCommit(rpcMessage)
			case messages.Rebase:
				p.handleRebase(rpcMessage)
			case messages.GetDownloadLocations:
				p.handleGetDownloadLocations(rpcMessage)
			case messages.RefreshTableFileUrl:
				p.handleRefreshTableFileUrl(rpcMessage)
			default:
				log.Println("Message", rpcMessage, "not handled")
			}
		case *messages.ChunkMessage:
			chunkMessage := message.(*messages.ChunkMessage)
			switch chunkMessage.PayloadType {
			case messages.RequestDownload:
				p.handleRequestDownload(chunkMessage)
			case messages.RequestUpload:
				p.handleRequestUpload(chunkMessage)
			default:
				log.Println("Message", chunkMessage, "not handled")
			}
		}
	}
}

func (p *ResponseProxy) handleGetRepoMetadataRequest(in *messages.RPCMessage) {
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

func (p *ResponseProxy) handleRootRequest(in *messages.RPCMessage) {
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

func (p *ResponseProxy) handleHasChunks(in *messages.RPCMessage) {
	request, err := messages.UnWrapHasChunksRequest(in)
	if err != nil {
		log.Println("Could not unwrap HasChunksRequest from consumed message")
	} else {
		response, err := p.client.HasChunks(p.context, request)
		if err != nil {
			log.Println("Could not request gRPC:", err.Error())
		} else {
			out, err := messages.WrapHasChunksResponse(response, in.ID)
			p.produceWithErrorLogging(err, out)
		}
	}
}

func (p *ResponseProxy) handleListTableFiles(in *messages.RPCMessage) {
	request, err := messages.UnWrapListTableFilesRequest(in)
	if err != nil {
		log.Println("Could not unwrap ListTableFilesRequest from consumed message")
	} else {
		response, err := p.client.ListTableFiles(p.context, request)
		if err != nil {
			log.Println("Could not request gRPC:", err.Error())
		} else {
			out, err := messages.WrapListTableFilesResponse(response, in.ID)
			p.produceWithErrorLogging(err, out)
		}
	}
}

func (p *ResponseProxy) handleGetUploadLocations(in *messages.RPCMessage) {
	request, err := messages.UnWrapGetUploadLocsRequest(in)
	if err != nil {
		log.Println("Could not unwrap GetUploadLocationsRequest from consumed message")
	} else {
		response, err := p.client.GetUploadLocations(p.context, request)
		if err != nil {
			log.Println("Could not request gRPC:", err.Error())
		} else {
			out, err := messages.WrapGetUploadLocsResponse(response, in.ID)
			p.produceWithErrorLogging(err, out)
		}
	}
}

func (p *ResponseProxy) handleAddTableFiles(in *messages.RPCMessage) {
	request, err := messages.UnWrapAddTableFilesRequest(in)
	if err != nil {
		log.Println("Could not unwrap AddTAbleFilesRequest from consumed message")
	} else {
		response, err := p.client.AddTableFiles(p.context, request)
		if err != nil {
			log.Println("Could not request gRPC:", err.Error())
		} else {
			out, err := messages.WrapAddTableFilesResponse(response, in.ID)
			p.produceWithErrorLogging(err, out)
		}
	}
}

func (p *ResponseProxy) handleCommit(in *messages.RPCMessage) {
	request, err := messages.UnWrapCommitRequest(in)
	if err != nil {
		log.Println("Could not unwrap CommitRequest from consumed message")
	} else {
		response, err := p.client.Commit(p.context, request)
		if err != nil {
			log.Println("Could not request gRPC:", err.Error())
		} else {
			out, err := messages.WrapCommitResponse(response, in.ID)
			p.produceWithErrorLogging(err, out)
		}
	}
}

func (p *ResponseProxy) handleRebase(in *messages.RPCMessage) {
	request, err := messages.UnWrapRebaseRequest(in)
	if err != nil {
		log.Println("Could not unwrap RebaseRequest from consumed message")
	} else {
		response, err := p.client.Rebase(p.context, request)
		if err != nil {
			log.Println("Could not request gRPC:", err.Error())
		} else {
			out, err := messages.WrapRebaseResponse(response, in.ID)
			p.produceWithErrorLogging(err, out)
		}
	}
}

func (p *ResponseProxy) handleGetDownloadLocations(in *messages.RPCMessage) {
	request, err := messages.UnWrapGetDownloadLocsRequest(in)
	if err != nil {
		log.Println("Could not unwrap GetDownloadLocationsRequest from consumed message")
	} else {
		response, err := p.client.GetDownloadLocations(p.context, request)
		if err != nil {
			log.Println("Could not request gRPC:", err.Error())
		} else {
			out, err := messages.WrapGetDownloadLocsResponse(response, in.ID)
			p.produceWithErrorLogging(err, out)
		}
	}
}

func (p *ResponseProxy) handleRefreshTableFileUrl(in *messages.RPCMessage) {
	request, err := messages.UnWrapRefreshTableFileUrlRequest(in)
	if err != nil {
		log.Println("Could not unwrap RefreshTableFileUrlRequest from consumed message")
	} else {
		response, err := p.client.RefreshTableFileUrl(p.context, request)
		if err != nil {
			log.Println("Could not request gRPC:", err.Error())
		} else {
			out, err := messages.WrapRefreshTableFileUrlResponse(response, in.ID)
			p.produceWithErrorLogging(err, out)
		}
	}
}

func (p *ResponseProxy) handleRequestDownload(message *messages.ChunkMessage) {
	request, err := http.NewRequest(http.MethodGet, message.URL, bytes.NewReader(message.Body))
	if err != nil {
		if err != nil {
			log.Println("Could not create request for URL:", message.URL, "error:", err.Error())
			return
		}
	}
	for headerName, headerValue := range message.Headers {
		request.Header.Add(headerName, headerValue)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Println("Could not get information from:", message.URL, "error:", err.Error())
		return
	}
	endOfTx := false
	contents := bytes.Buffer{}
	totalRead := 0
	for !endOfTx {
		buf := make([]byte, 4096)
		read, err := response.Body.Read(buf)
		totalRead += read
		endOfTx = err == io.EOF
		if err != nil && err != io.EOF {
			log.Println("Could not read information into buffer:", err.Error())
			return
		}
		contents.Write(buf[:read])
		log.Println("Read", read, "bytes into buffer for chunk:", message.String())
	}
	log.Println("Writing", totalRead, "bytes back to caller for chunk:", message.String())
	out := messages.NewResponseChunkMessage(
		message.PayloadType, contents.Bytes(), make(map[string]string), message.Identifier())
	err = p.produceMessage(out)
	if err != nil {
		log.Println("Could not send response back to caller:", err.Error())
	}
}

func (p *ResponseProxy) handleRequestUpload(message *messages.ChunkMessage) {
	request, err := http.NewRequest(http.MethodPut, message.URL, bytes.NewReader(message.Body))
	if err != nil {
		log.Println("Could not create request for:", message.URL, "error:", err.Error())
		return
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Println("Could not put information to:", message.URL, "error:", err.Error())
		return
	}
	if response.StatusCode >= 400 {
		log.Println("Could not put information to:", message.URL, "status:", response.Status)
		return
	}
	out := messages.NewResponseChunkMessage(
		message.PayloadType, []byte{}, make(map[string]string), message.Identifier())
	err = p.produceMessage(out)
	if err != nil {
		log.Println("Could not send response back to caller:", err.Error())
	}
}

func (p *ResponseProxy) handleStreamDownloadLocations(in *messages.RPCMessage) {
	if in.EndOfTx {
		log.Println("Deleting streaming client for:", in.ID.String())
		clientEx, clientRegistered := p.downloadLocationsClientExs[in.ID]
		if clientRegistered {
			err := clientEx.client.CloseSend()
			if err != nil {
				log.Println("Closing client send operation failed:", err.Error())
			}
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
	in *messages.RPCMessage,
	clientEx StreamDownloadLocationsClientEx,
) {

	for {
		response, err := clientEx.client.Recv()
		log.Println("Received new streaming response for:", in.String())
		if err != nil {
			if err == io.EOF {
				log.Println("No more responses on streaming download locations for", in.String())
				out := messages.NewStreamingRPCMessageEndTransmission(messages.StreamDownloadLocations, in.ID)
				err := p.produceMessage(out)
				if err != nil {
					log.Println("Received error while sending EOT for", in.String())
				}
			} else {
				log.Println("Received error while receiving download locations for", in.String())
			}
			clientEx.wg.Done()
			return
		}
		out, err := messages.WrapStreamingDownloadLocsResponse(response, in.ID)
		log.Println(response.Locs)
		p.produceWithErrorLogging(err, out)
	}
}

func (p *ResponseProxy) produceWithErrorLogging(err error, out messages.Message) {
	if err != nil {
		log.Println("Received error while wrapping:", err.Error())
	} else {
		err := p.produceMessage(out)
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
		TopicPartition: kafka.TopicPartition{Topic: &p.config.ProducingTopic, Partition: kafka.PartitionAny},
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
	if p.isStarted {
		p.rpcConsumer.Close()
		p.isStarted = false
	}
}
