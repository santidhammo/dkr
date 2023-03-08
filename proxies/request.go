package proxies

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
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
	producer        *kafka.Producer
	responseChannel map[messages.MessageID]chan messages.Message
	mutex           sync.Mutex
	config          *config.RequestProxyConfig
	consumer        *MessageConsumer
	isStarted       bool
	httpServer      *http.Server
	pb.UnimplementedChunkStoreServiceServer
}

func NewRequestProxy(config *config.RequestProxyConfig) (*RequestProxy, error) {
	producer, err := kafka.NewProducer(config.ProducerKafkaConfig)
	if err != nil {
		return nil, err
	}

	requestProxy := &RequestProxy{
		producer:        producer,
		responseChannel: make(map[messages.MessageID]chan messages.Message),
		config:          config,
		consumer:        nil,
		httpServer:      nil,
		isStarted:       false,
	}
	requestProxy.consumer = NewMessageConsumer(
		config.ConsumerKafkaConfig,
		config.ConsumingTopic,
		func(m messages.Message) {
			requestProxy.dispatchMessage(m)
		})

	requestProxy.httpServer = &http.Server{
		Addr:    ":" + strconv.Itoa(int(config.HttpPort)),
		Handler: requestProxy,
	}
	return requestProxy, nil
}

func (p *RequestProxy) Start() {
	if !p.isStarted {
		go p.consumer.Start()
		// TODO: add TLS version here as well
		go func() {
			err := p.httpServer.ListenAndServe()
			if err != nil {
				log.Fatalln("Could not start HTTP Server:", err.Error())
			}
		}()
		p.isStarted = true
	}
}

func (p *RequestProxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	log.Println("Processing HTTP Chunk request:", request.URL.String())
	payloadTypeString := getRequestPayloadTypeString(request)
	chunkURL := recombineURL(*request.URL)
	log.Println("Recombined HTTP Chunk URL:", chunkURL.String())
	headers := make(map[string]string)
	headers["Range"] = request.Header.Get("Range")

	if isValidDownloadPayloadType(payloadTypeString) {
		produceMessage := messages.NewTrackedChunkMessage(
			messages.RequestDownload, []byte{}, chunkURL.String(), headers)
		response, err := p.singleShotProduceAndConsumeMessage(produceMessage)
		if err != nil {
			log.Println("Received error on awaiting response:", err.Error())
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		result, ok := response.(*messages.ChunkMessage)
		buffer := bytes.Buffer{}
		written := 0
		if ok {
			written, err = buffer.Write(result.Body)
			if err != nil {
				log.Println("Body chunk could not be written back to caller:", err.Error())
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
		} else {
			log.Println("Decoded message is not a valid Chunk Message")
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}

		chunk := buffer.Bytes()[:written]
		_, err = writer.Write(chunk)
		if err != nil {
			log.Println("Could not write response data back")
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Println("Returned", written, "bytes to caller for:", result.String())
	} else if isValidUploadPayloadType(payloadTypeString) {
		contents := bytes.Buffer{}
		totalRead := 0
		endOfTx := false
		for !endOfTx {
			buf := make([]byte, 4096)
			read, err := request.Body.Read(buf)
			endOfTx = err == io.EOF
			if err != nil && err != io.EOF {
				log.Println("Could not read body")
				writer.WriteHeader(http.StatusNotAcceptable)
				return
			}
			contents.Write(buf[:read])
			totalRead += read
		}

		produceMessage := messages.NewTrackedChunkMessage(
			messages.RequestUpload, contents.Bytes(), chunkURL.String(), headers)

		_, err := p.singleShotProduceAndConsumeMessage(produceMessage)
		if err != nil {
			log.Println("Received error on awaiting response:", err.Error())
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err != nil {
			log.Println("Could not write response data back")
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else {
		log.Println("URL encoding not accepted")
		writer.WriteHeader(http.StatusInternalServerError)
	}

}

func isValidUploadPayloadType(payloadTypeString string) bool {
	payloadTypeGetUploadLocations := messages.GetUploadLocations
	return payloadTypeString == payloadTypeGetUploadLocations.String()
}

func isValidDownloadPayloadType(payloadTypeString string) bool {
	payloadTypeStreamDownloadLocations := messages.StreamDownloadLocations
	payloadTypeListTableFiles := messages.ListTableFiles
	payloadTypeRefreshTableFileUrl := messages.RefreshTableFileUrl
	return payloadTypeString == payloadTypeStreamDownloadLocations.String() ||
		payloadTypeString == payloadTypeListTableFiles.String() ||
		payloadTypeString == payloadTypeRefreshTableFileUrl.String()
}

func getRequestPayloadTypeString(request *http.Request) string {
	return request.URL.Query().Get("payload_type")
}

func recombineURL(sourceUrl url.URL) url.URL {
	q := sourceUrl.Query()
	originHost := q.Get("origin_host")
	originScheme := q.Get("origin_scheme")
	q.Del("origin_host")
	q.Del("origin_scheme")
	q.Del("payload_type")
	u := *&sourceUrl // Create a copy
	u.Host = originHost
	u.Scheme = originScheme
	u.RawQuery = q.Encode()
	return u
}

func (p *RequestProxy) GetRepoMetadata(_ context.Context, in *pb.GetRepoMetadataRequest) (*pb.GetRepoMetadataResponse, error) {
	inMessage, err := messages.WrapGetRepoMetadataRequest(in)
	if err == nil {
		outMessage, err := p.singleShotProduceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapGetRepoMetadataResponse(outMessage.(*messages.RPCMessage))
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) HasChunks(_ context.Context, in *pb.HasChunksRequest) (*pb.HasChunksResponse, error) {
	inMessage, err := messages.WrapHasChunksRequest(in)
	if err == nil {
		outMessage, err := p.singleShotProduceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapHasChunksResponse(outMessage.(*messages.RPCMessage))
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
		outMessage, err := p.singleShotProduceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapGetDownloadLocsResponse(outMessage.(*messages.RPCMessage))
			if err != nil {
				return response, err
			}
			err = p.replaceDownloadChunkUrls(ctx, response)
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
		outMessage, err := p.singleShotProduceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapGetUploadLocsResponse(outMessage.(*messages.RPCMessage))
			for _, location := range response.Locs {
				md, _ := metadata.FromIncomingContext(ctx)
				switch location.Location.(type) {
				case *pb.UploadLoc_HttpPost:
					u, err := url.Parse(location.Location.(*pb.UploadLoc_HttpPost).HttpPost.Url)
					if err != nil {
						log.Println("Could not properly parse URL:", err.Error())
					}
					newUrl := p.transformUrl(md, u, outMessage.(*messages.RPCMessage).PayloadType)
					location.Location.(*pb.UploadLoc_HttpPost).HttpPost.Url = newUrl.String()
				default:
					return nil, errors.New("could not mutate upload location, not of type UploadLoc_HttpPost")
				}
			}
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) Rebase(_ context.Context, in *pb.RebaseRequest) (*pb.RebaseResponse, error) {
	inMessage, err := messages.WrapRebaseRequest(in)
	if err == nil {
		outMessage, err := p.singleShotProduceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapRebaseResponse(outMessage.(*messages.RPCMessage))
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) Root(_ context.Context, in *pb.RootRequest) (*pb.RootResponse, error) {
	inMessage, err := messages.WrapRootRequest(in)
	if err == nil {
		outMessage, err := p.singleShotProduceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapRootResponse(outMessage.(*messages.RPCMessage))
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) Commit(_ context.Context, in *pb.CommitRequest) (*pb.CommitResponse, error) {
	inMessage, err := messages.WrapCommitRequest(in)
	if err == nil {
		outMessage, err := p.singleShotProduceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapCommitResponse(outMessage.(*messages.RPCMessage))
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
		outMessage, err := p.singleShotProduceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapListTableFilesResponse(outMessage.(*messages.RPCMessage))
			for _, info := range response.TableFileInfo {
				md, _ := metadata.FromIncomingContext(ctx)
				u, err := url.Parse(info.Url)
				if err != nil {
					log.Println("Could not properly parse URL:", err.Error())
				}
				newUrl := p.transformUrl(md, u, outMessage.(*messages.RPCMessage).PayloadType)
				info.Url = newUrl.String()
			}
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
		outMessage, err := p.singleShotProduceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapRefreshTableFileUrlResponse(outMessage.(*messages.RPCMessage))
			md, _ := metadata.FromIncomingContext(ctx)
			u, err := url.Parse(response.Url)
			if err != nil {
				log.Println("Could not properly parse URL:", err.Error())
			}
			newUrl := p.transformUrl(md, u, outMessage.(*messages.RPCMessage).PayloadType)
			response.Url = newUrl.String()
			return response, err
		} else {
			return nil, err
		}
	}
	return nil, err
}

func (p *RequestProxy) AddTableFiles(_ context.Context, in *pb.AddTableFilesRequest) (*pb.AddTableFilesResponse, error) {
	inMessage, err := messages.WrapAddTableFilesRequest(in)
	if err == nil {
		outMessage, err := p.singleShotProduceAndConsumeMessage(inMessage)
		if err == nil {
			response, err := messages.UnWrapAddTableFilesResponse(outMessage.(*messages.RPCMessage))
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
	p.ensureResponseChannel(id)

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

		var request *pb.GetDownloadLocsRequest
		request, err = stream.Recv()
		if err == io.EOF {
			message := messages.NewStreamingRPCMessageEndTransmission(messages.StreamDownloadLocations, id)
			p.produceAndVerifyMessage(message)
			return
		} else if err != nil {
			log.Print("Receiving further data failed:", err.Error())
			return
		} else {
			var message *messages.RPCMessage
			message, err = messages.WrapStreamingDownloadLocsRequest(request, id)
			if err != nil {
				return
			}

			if !p.produceAndVerifyMessage(message) {
				return
			}
		}
	}
}

func (p *RequestProxy) streamDownloadLocationsSender(id messages.MessageID, stream pb.ChunkStoreService_StreamDownloadLocationsServer, wg *sync.WaitGroup, errs chan error) {
	defer func() {
		wg.Done()
		close(errs)
	}()
	channel := p.responseChannel[id]
	if channel != nil {
		for {
			log.Println("Awaiting next message for RPCMessage ID:", id.String())
			message := <-channel
			if message.EndOfTransaction() {
				return
			}
			rpcMessage, ok := message.(*messages.RPCMessage)
			if ok {
				response, err := messages.UnWrapStreamingDownloadLocsResponse(rpcMessage)
				if err != nil {
					errs <- err
					return
				}
				err = p.replaceDownloadChunkUrls(stream.Context(), response)
				if err != nil {
					errs <- err
					return
				}

				err = stream.Send(response)
				if err != nil {
					errs <- err
					return
				}
			} else {
				log.Println("Message could not be converted to RPC message")
			}
		}
	}
}

func (p *RequestProxy) replaceDownloadChunkUrls(ctx context.Context, response *pb.GetDownloadLocsResponse) error {
	md, _ := metadata.FromIncomingContext(ctx)

	for _, loc := range response.Locs {
		oldGet := loc.GetHttpGet()
		if oldGet != nil {
			u, err := url.Parse(oldGet.Url)
			if err != nil {
				return err
			}

			replacedUrl := p.transformUrl(md, u, messages.StreamDownloadLocations)
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
				return err
			}
			replacedUrl := p.transformUrl(md, u, messages.StreamDownloadLocations)
			log.Println("Replaced url:", u, "with:", replacedUrl)

			newGetRange := pb.HttpGetRange{}
			newGetRange.Ranges = oldGetRange.Ranges
			newGetRange.Url = replacedUrl.String()

			loc.Location = &pb.DownloadLoc_HttpGetRange{HttpGetRange: &newGetRange}
		}
	}
	return nil
}

/*
ensureResponseChannel ensures that a channel exists for the appropriate kind of responses.

When producing a message onto the request Kafka queue, the result will be posted on a different Kafka queue. As this
is asynchronous behaviour, it needs to be rerouted back to the original request function. By using channels, the
original function call will retrieve the associated response from Kafka
*/
func (p *RequestProxy) ensureResponseChannel(id messages.MessageID) {
	// A mutex is used as a channel should never be created twice and overwrite an existing channel
	p.mutex.Lock()
	defer p.mutex.Unlock()

	_, ok := p.responseChannel[id]
	if !ok {
		log.Println("Create message channel for Message ID:", id.String())
		p.responseChannel[id] = make(chan messages.Message)
	}
}

func (p *RequestProxy) dispatchMessage(message messages.Message) {
	responseChannel := p.responseChannel[message.Identifier()]

	if responseChannel != nil {
		log.Println("Dispatching message:", message.String())
		select {
		case responseChannel <- message:
			log.Println("Dispatched message:", message.String())
		case <-time.After(p.config.MaxAwaitDuration):
			log.Println("Dispatching message:", message.String(), "failed, received timeout")
		}
	} else {
		log.Println("Ignore message:", message.String())
	}

	if message.EndOfTransaction() {
		p.removeIDFromResponseChannel(message.Identifier())
	}
}

func (p *RequestProxy) singleShotProduceAndConsumeMessage(message messages.Message) (messages.Message, error) {
	if message != nil {
		producerChannel, err := p.produceMessage(message)
		if err != nil {
			return nil, err
		}
		for {
			ok := p.awaitProductionSuccess(producerChannel)
			if ok {
				return p.awaitResponse(message.Identifier())
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
		TopicPartition: kafka.TopicPartition{Topic: &p.config.ProducingTopic, Partition: kafka.PartitionAny},
		Value:          encoded,
	}
	p.ensureResponseChannel(message.Identifier())
	producerChannel := make(chan kafka.Event)
	log.Println("Producing message:", message.String())
	err = p.producer.Produce(&kafkaMessage, producerChannel)
	if err != nil {
		return nil, err
	}
	return producerChannel, nil
}

func (p *RequestProxy) awaitProductionSuccess(producerChannel chan kafka.Event) bool {
	for {
		e := <-producerChannel
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				origin, err := messages.Decode(ev.Value)
				if err != nil {
					log.Println("Could not determine value of event: ", err.Error())
				} else {
					p.removeIDFromResponseChannel(origin.Identifier())
				}
				log.Println("Received error while producing on kafka topic:", ev.TopicPartition.Error.Error())
				return false
			}
			return true
		}
	}
}

func (p *RequestProxy) awaitResponse(id messages.MessageID) (messages.Message, error) {
	log.Println("Awaiting consumption of message:", id.String())
	rpcChannel, ok := p.responseChannel[id]
	if ok {
		select {
		case decodedMessage := <-rpcChannel:
			if decodedMessage.EndOfTransaction() {
				p.removeIDFromResponseChannel(id)
			}
			return decodedMessage, nil
		case <-time.After(p.config.MaxAwaitDuration):
			p.removeIDFromResponseChannel(id)
			// TODO: should create proper error
			return nil, errors.New("did not receive response after request/response timeout for message")
		}
	} else {
		return nil, errors.New("no associated channel found for the given message identifier")
	}
}

func (p *RequestProxy) produceAndVerifyMessage(message messages.Message) bool {
	producerChannel, err := p.produceMessage(message)
	if err != nil {
		log.Println("Could not produce message:", err.Error())
		return false
	}
	return p.awaitProductionSuccess(producerChannel)
}

func (p *RequestProxy) removeIDFromResponseChannel(id messages.MessageID) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	_, ok := p.responseChannel[id]
	if ok {
		delete(p.responseChannel, id)
	}
}

func (p *RequestProxy) transformUrl(md metadata.MD, u *url.URL, payloadType messages.RPCPayloadType) *url.URL {
	host := p.getHost(md)

	q := u.Query()
	q.Add("origin_scheme", u.Scheme)
	q.Add("origin_host", u.Host)
	q.Add("payload_type", payloadType.String())

	return &url.URL{
		Scheme:   p.config.HttpScheme(),
		Host:     host,
		Path:     u.Path,
		RawQuery: q.Encode(),
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
	if p.isStarted {
		p.consumer.Close()
		_ = p.httpServer.Close()
		p.isStarted = false
	}
}
