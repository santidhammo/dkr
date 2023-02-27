package config

import (
	"flag"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
)

type RequestProxyConfig struct {
	ProducerKafkaConfig  *kafka.ConfigMap
	ConsumerKafkaConfig  *kafka.ConfigMap
	Directory            string
	ExternalHttpPort     uint
	ExternalHttpHost     string
	HttpPort             uint
	GrpcPort             uint
	UseTLS               bool
	PublicKey            []byte
	PrivateKey           []byte
	PrivateKeyPassphrase string
	ProducingRPCTopic    string
	ConsumingRPCTopic    string
	ProducingChunkTopic  string
	ConsumingChunkTopic  string
}

func (c *RequestProxyConfig) HttpScheme() string {
	if c.UseTLS {
		return "https"
	} else {
		return "http"
	}
}

var requestProxyConfig *RequestProxyConfig = nil

func GetRequestProxyConfig() *RequestProxyConfig {
	if requestProxyConfig != nil {
		return requestProxyConfig
	}

	directory := flag.String("dir", "", "Directory which contains the chunk storage, if not set, uses the current working directory")
	httpPort := flag.Uint("http-port", 0, "The port the HTTP server will listen on (default 8080 if the server is insecure, default 8443 if the server is secure)")
	externalHttpHost := flag.String("external-http-host", "", "The host name to expose (defaults to the host part of the :authority header if not set explicitly)")
	externalHttpPort := flag.Uint("external-http-port", 0, "The host port to expose (defaults to the port part of the :authority header if not set explicitly)")
	grpcPort := flag.Uint("grpc-port", 50051, "The port the gRPC server will listen on")
	useTLS := flag.Bool("tls", false, "Use transport layer security, encrypt traffic between this and the client/server")
	publicKeyFile := flag.String("public-key", "", "Use the given public key for TLS encryption")
	privateKeyFile := flag.String("private-key", "", "Use the given private key for TLS encryption")
	privateKeyPassphrase := flag.String("private-key-passphrase", "", "Use this passphrase to decrypt the private key for TLS encryption")
	producingRPCTopic := flag.String("producing-rpc-topic", "dkr-request", "This topic is used to send the requests to the response-proxy")
	consumingRPCTopic := flag.String("consuming-rpc-topic", "dkr-response", "This topic is used to retrieve the responses from the response-proxy")
	producingChunkTopic := flag.String("producing-chunk-topic", "dkr-request-chunk", "This topic is used to send chunks to the response-proxy")
	consumingChunkTopic := flag.String("consuming-chunk-topic", "dkr-response-chunk", "This topic is used to retrieve chunks from the response-proxy")

	flag.Parse()

	if *useTLS {
		if httpPort == nil || *httpPort == 0 {
			*httpPort = 8443
		} else {
			verifyPort(*httpPort)
		}
		log.Println("Using HTTPS port:", *httpPort)
	} else {
		if httpPort == nil || *httpPort == 0 {
			*httpPort = 8080
		} else {
			verifyPort(*httpPort)
		}
		log.Println("Using HTTP port:", *httpPort)
	}

	verifyPort(*grpcPort)
	log.Println("Using gRPC port:", *grpcPort)

	if externalHttpHost != nil && len(*externalHttpHost) > 0 {
		if *useTLS {
			log.Println("Using custom Chunk HTTPS host name:", *externalHttpHost)
		} else {
			log.Println("Using custom Chunk HTTP host name:", *externalHttpHost)
		}
	}

	if externalHttpPort != nil && *externalHttpPort != 0 {
		verifyPort(*externalHttpPort)
		log.Println("Using custom Chunk HTTP(S) port:", *externalHttpPort)
	}

	var publicKey []byte
	var privateKey []byte

	if *useTLS {
		publicKey, privateKey = loadKeys(publicKeyFile, privateKeyFile)
		if len(publicKey) == 0 {
			log.Fatalln("Loaded public key is empty from file:", publicKeyFile)
		}

		if len(privateKey) == 0 {
			log.Fatalln("Loaded private key is empty from file:", privateKeyFile)
		}
		log.Println("Using TLS with public key:", *publicKeyFile, "and key:", *privateKeyFile)
		if privateKeyPassphrase == nil || len(*privateKeyPassphrase) == 0 {
			log.Fatalln("Private key passphrase can not be empty, please set -private-key-passphrase")
		}
	}

	producerKafkaConfig, consumerKafkaConfig := GetKafkaConfigMap()

	requestProxyConfig = &RequestProxyConfig{
		ProducerKafkaConfig:  producerKafkaConfig,
		ConsumerKafkaConfig:  consumerKafkaConfig,
		Directory:            *directory,
		HttpPort:             *httpPort,
		ExternalHttpHost:     *externalHttpHost,
		ExternalHttpPort:     *externalHttpPort,
		GrpcPort:             *grpcPort,
		UseTLS:               *useTLS,
		PublicKey:            publicKey,
		PrivateKey:           privateKey,
		PrivateKeyPassphrase: *privateKeyPassphrase,
		ProducingRPCTopic:    *producingRPCTopic,
		ConsumingRPCTopic:    *consumingRPCTopic,
		ProducingChunkTopic:  *producingChunkTopic,
		ConsumingChunkTopic:  *consumingChunkTopic,
	}

	return requestProxyConfig
}

func verifyPort(input uint) {
	// If the port is a value outside the valid range of a TCP port, then error out
	if input == 0 || input > 65535 {
		log.Fatalln("A TCP port should be a valid number within the range 1..65535, was:", input)
	}
}

func loadKeys(publicKeyFile *string, privateKeyFile *string) ([]byte, []byte) {
	// If the keys are absent by name, or the referenced file is absent (and can not be loaded) abort, otherwise
	// return the file's contents.
	if publicKeyFile == nil || len(*publicKeyFile) == 0 {
		log.Fatalln("Public Key file location must be declared using option -public-key")
	}

	if privateKeyFile == nil || len(*privateKeyFile) == 0 {
		log.Fatalln("Private Key file location must be declared using option -private-key")
	}

	publicKey, err := os.ReadFile(*publicKeyFile)
	if err != nil {
		log.Fatalln("Public Key could not be read from:", *publicKeyFile, " error:", err.Error())
	}

	privateKey, err := os.ReadFile(*publicKeyFile)
	if err != nil {
		log.Fatalln("Private Key could not be read from:", *privateKeyFile, " error:", err.Error())
	}

	return publicKey, privateKey
}
