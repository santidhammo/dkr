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

	ConsumingTopic string
	ProducingTopic string
}

var responseProxyConfig *ResponseProxyConfig = nil

func GetResponseProxyConfig() *ResponseProxyConfig {
	if responseProxyConfig != nil {
		return responseProxyConfig
	}

	directory := flag.String("dir", "", "Directory which contains the chunk storage, if not set, uses the current working directory")
	remoteURL := flag.String("remote-url", "localhost:50051", "URL with the endpoint of the remote")

	consumingTopic := flag.String("consuming-topic", "dkr-request", "This topic is used to retrieve the requests from the request-proxy")
	producingTopic := flag.String("producing-topic", "dkr-response", "This topic is used to send the responses to the request-proxy")

	flag.Parse()

	producerKafkaConfig, consumerKafkaConfig := GetKafkaConfigMap()

	responseProxyConfig = &ResponseProxyConfig{
		ProducerKafkaConfig: producerKafkaConfig,
		ConsumerKafkaConfig: consumerKafkaConfig,
		Directory:           *directory,
		RemoteURL:           *remoteURL,
		ConsumingTopic:      *consumingTopic,
		ProducingTopic:      *producingTopic,
	}

	return responseProxyConfig
}
