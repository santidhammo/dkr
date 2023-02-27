package config

import (
	"flag"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ResponseProxyConfig struct {
	ProducerKafkaConfig *kafka.ConfigMap
	ConsumerKafkaConfig *kafka.ConfigMap

	Directory string
	RemoteURL string

	ConsumingRPCTopic   string
	ProducingRPCTopic   string
	ConsumingChunkTopic string
	ProducingChunkTopic string
}

var responseProxyConfig *ResponseProxyConfig = nil

func GetResponseProxyConfig() *ResponseProxyConfig {
	if responseProxyConfig != nil {
		return responseProxyConfig
	}

	directory := flag.String("dir", "", "Directory which contains the chunk storage, if not set, uses the current working directory")
	remoteURL := flag.String("remote-url", "localhost:50051", "URL with the endpoint of the remote")

	consumingRPCTopic := flag.String("consuming-rpc-topic", "dkr-request", "This topic is used to retrieve the requests from the request-proxy")
	producingRPCTopic := flag.String("producing-rpc-topic", "dkr-response", "This topic is used to send the responses to the request-proxy")
	consumingChunkTopic := flag.String("consuming-chunk-topic", "dkr-request-chunk", "This topic is used to retrieve chunks from the request-proxy")
	producingChunkTopic := flag.String("producing-chunk-topic", "dkr-response-chunk", "This topic is used to send chunks to the request-proxy")

	flag.Parse()

	producerKafkaConfig, consumerKafkaConfig := GetKafkaConfigMap()

	responseProxyConfig = &ResponseProxyConfig{
		ProducerKafkaConfig: producerKafkaConfig,
		ConsumerKafkaConfig: consumerKafkaConfig,
		Directory:           *directory,
		RemoteURL:           *remoteURL,
		ConsumingRPCTopic:   *consumingRPCTopic,
		ProducingRPCTopic:   *producingRPCTopic,
		ConsumingChunkTopic: *consumingChunkTopic,
		ProducingChunkTopic: *producingChunkTopic,
	}

	return responseProxyConfig
}
