package config

import (
	"flag"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"log"
	"os"
	"strings"
)

var producerConfigMap *kafka.ConfigMap = nil
var consumerConfigMap *kafka.ConfigMap = nil

var kafkaClientId = flag.String("kafka-client-id", "dkr", "Kafka client identifier (Confluent: client.id)")
var kafkaMetadataBrokerList = flag.String("kafka-metadata-broker-list", "", "Initial list of brokers (Confluent: metadata.broker.list)")
var kafkaDebug = flag.String("kafka-debug", "", "A comma-seaprated list of Kafka debug options to enalbe (Confluent: debug)")
var kafkaSocketTimeoutMs = flag.Uint("kafka-socket-timeout-ms", 60000, "Default timeout for network requests (Confluent: socket.timeout.ms)")
var kafkaSocketKeepalive = flag.Bool("kafka-socket-keepalive", false, "Enable TCP keepalive (Confluent: socket.keepalive.enable)")
var kafkaSocketNoNagle = flag.Bool("kafka-socket-no-nagle", false, "Disable the Nagle protocol (Confluent: socket.nagle.disable)")
var kafkaBrokerAddressFamily = flag.String("kafka-broker-address-family", "any", "Allow broker address family (Confluent: broker.address.family)")
var kafkaLogLevel = flag.Uint("kafka-log-level", 6, "Kafka syslog level (Confluent: log_level)")
var kafkaSecurityProtocol = flag.String("kafka-security-protocol", "plaintext", "Protocol used to communicate with brokers (Confluent: security.protocol)")
var kafkaPrivateKeyFile = flag.String("kafka-private-key", "", "Kafka Private Key file location (Confluent: ssl.key.location)")
var kafkaPrivateKeyPassphrase = flag.String("kafka-private-key-passphrase", "", "Kafka Private Key passphrase (Confluent: ssl.key.password)")
var kafkaPublicKeyFile = flag.String("kafka-public-key", "", "Kafka Public Key file location (Confluent: ssl.certificate.location)")
var kafkaCAPath = flag.String("kafka-ca-path", "", "Kafka Certificate Authority path (Confluent: ssl.ca.location)")
var kafkaVerifyCertificates = flag.Bool("kafka-verify-certificates", true, "Enable certificate verification (Confluent: enable.ssl.certificate.verification)")
var kafkaGroupIDAsPrefix = flag.Bool("kafka-group-id-as-prefix", true, "Uses the group ID as prefix, otherwise enables group rebalancing on scaling")
var kafkaGroupID = flag.String("kafka-group-id", "dkr", "The Kafka Group ID (Confluent: group.id)")
var kafkaGroupInstanceID = flag.String("kafka-group-instance-id", "", "THe Kafka Group instance ID for quick rejoin (Confluent: group.instance.id)")
var kafkaCompressionCodec = flag.String("kafka-compression-codec", "none", "The compression codec to use (Confluent: compression.codec)")
var kafkaSASLMechanism = flag.String("kafka-sasl-mechanism", "GSSAPI", "Kafka SASL mechanism (Confluent: sasl.mechanism)")
var kafkaSASLKerberosServiceName = flag.String("kafka-sasl-kerberos-service-name", "kafka", "Kafka kerberos service name (Confluent: sasl.kerberos.service.name)")
var kafkaSASLKerberosPrincipal = flag.String("kafka-sasl-kerberos-principal", "kafkaclient", "Kafka kerberos principal (Confluent: sasl.kerberos.principal)")
var kafkaSASLKerberosKeytabFile = flag.String("kafka-sasl-keytab-file", "", "Path to the Kerberos keytab (Confluent: sasl.kerberos.keytab)")
var kafkaSASLUsername = flag.String("kafka-sasl-username", "", "The SASL username (Confluent: sasl.username)")
var kafkaSASLPassword = flag.String("kafka-sasl-password", "", "The SASL password (Confluent: sasl.password)")
var kafkaSASLOAuthBearerConfig = flag.String("kafka-sasl-oauth-bearer-config", "", "The OAuth bearer configuration (Confluent: sasl.oauthbearer.config")
var kafkaSASLOAuthBearerUnsecureJWT = flag.Bool("kafka-sasl-oauth-bearer-unsecure-jwt", false, "The OAuth bearer unsecure JWT (Confluent: enable.sasl.oauthbearer.unsecure.jwt)")
var kafkaSASLOAuthBearerMethod = flag.String("kafka-sasl-oauth-bearer-method", "default", "The OAuth bearer method, default or oidc (Confluent: sasl.oathbearer.method)")
var kafkaSASLOAuthBearerClientID = flag.String("kafka-sasl-oauth-bearer-client-id", "default", "The OAuth bearer Client ID (Confluent: sasl.oathbearer.client.id)")
var kafkaSASLOAuthBearerClientSecretFile = flag.String("kafka-sasl-oauth-bearer-client-secret-file", "", "The OAuth bearer Client Secret (Confluent: sasl.oathbearer.client.secret)")
var kafkaSASLOAuthBearerScope = flag.String("kafka-sasl-oauth-bearer-scope", "", "The OAuth bearer Client Secret (Confluent: sasl.oathbearer.scope)")
var kafkaSASLOAuthBearerExtensions = flag.String("kafka-sasl-oauth-bearer-extensions", "", "The OAuth bearer extensions (Confluent: sasl.oauthbearer.extensions)")
var kafkaSASLOAuthBearerTokenEndpointURL = flag.String("kafka-sasl-oauth-bearer-token-endpoint-url", "", "The OAuth bearer endpoint URL (Confluent: sasl.oauthbearer.token.endpoint.url)")

func GetKafkaConfigMap() (*kafka.ConfigMap, *kafka.ConfigMap) {
	if producerConfigMap != nil && consumerConfigMap != nil {
		return producerConfigMap, consumerConfigMap
	}
	var kafkaSASLOAuthBearerClientSecret *string = nil
	if kafkaSASLOAuthBearerClientSecretFile != nil && len(*kafkaSASLOAuthBearerClientSecretFile) > 0 {
		rawSecret, err := os.ReadFile(*kafkaSASLOAuthBearerClientSecretFile)
		if err != nil {
			log.Fatalln("Could not load Kafka SASL OAuth Bearer Client Secret from:", kafkaSASLOAuthBearerClientSecretFile)
		}
		stringSecret := string(rawSecret)
		kafkaSASLOAuthBearerClientSecret = &stringSecret
	}

	if kafkaMetadataBrokerList == nil || len(*kafkaMetadataBrokerList) == 0 {
		log.Fatalln("No brokers set in -kafka-metadata-broker-list, must at least one broker in comma separated format")
	}

	var configMaps []*kafka.ConfigMap
	consumerConfigMap = &kafka.ConfigMap{}
	producerConfigMap = &kafka.ConfigMap{}
	configMaps = append(configMaps, consumerConfigMap)
	configMaps = append(configMaps, producerConfigMap)

	for _, configMap := range configMaps {
		configMap.SetKey("client.id", *kafkaClientId)
		configMap.SetKey("metadata.broker.list", *kafkaMetadataBrokerList)
		if kafkaDebug != nil && len(*kafkaDebug) > 0 {
			configMap.SetKey("debug", *kafkaDebug)
		}
		configMap.SetKey("socket.timeout.ms", int(*kafkaSocketTimeoutMs))
		configMap.SetKey("socket.keepalive.enable", *kafkaSocketKeepalive)
		configMap.SetKey("socket.nagle.disable", *kafkaSocketNoNagle)
		configMap.SetKey("broker.address.family", *kafkaBrokerAddressFamily)
		configMap.SetKey("log_level", int(*kafkaLogLevel))
		configMap.SetKey("security.protocol", *kafkaSecurityProtocol)
		configMap.SetKey("ssl.key.location", *kafkaPrivateKeyFile)
		configMap.SetKey("ssl.key.password", *kafkaPrivateKeyPassphrase)
		configMap.SetKey("ssl.certificate.location", *kafkaPublicKeyFile)
		configMap.SetKey("ssl.ca.location", *kafkaCAPath)
		configMap.SetKey("enable.ssl.certificate.verification", *kafkaVerifyCertificates)

		if configMap == consumerConfigMap {
			determineAndSetGroupID(configMap)
			configMap.SetKey("group.instance.id", *kafkaGroupInstanceID)
			// We don't want old stuff when starting the consumer
			configMap.SetKey("auto.offset.reset", "latest")
		}
		if configMap == producerConfigMap {
			configMap.SetKey("compression.codec", *kafkaCompressionCodec)
		}

		if kafkaSecurityProtocol != nil && strings.HasPrefix(*kafkaSecurityProtocol, "SASL_") {
			configMap.SetKey("sasl.mechanism", *kafkaSASLMechanism)
			configMap.SetKey("sasl.kerberos.service.name", *kafkaSASLKerberosServiceName)
			configMap.SetKey("sasl.kerberos.principal", *kafkaSASLKerberosPrincipal)
			configMap.SetKey("sasl.kerberos.keytab", *kafkaSASLKerberosKeytabFile)
			configMap.SetKey("sasl.username", *kafkaSASLUsername)
			configMap.SetKey("sasl.password", *kafkaSASLPassword)
			configMap.SetKey("sasl.oauthbearer.config", *kafkaSASLOAuthBearerConfig)
			configMap.SetKey("enable.sasl.oauthbearer.unsecure.jwt", *kafkaSASLOAuthBearerUnsecureJWT)
			configMap.SetKey("sasl.oauthbearer.method", *kafkaSASLOAuthBearerMethod)
			configMap.SetKey("sasl.oauthbearer.client.id", *kafkaSASLOAuthBearerClientID)
			if kafkaSASLOAuthBearerClientSecret != nil {
				configMap.SetKey("sasl.oauthbearer.client.secret", *kafkaSASLOAuthBearerClientSecret)
			}
			configMap.SetKey("sasl.oauthbearer.scope", *kafkaSASLOAuthBearerScope)
			configMap.SetKey("sasl.oauthbearer.extensions", *kafkaSASLOAuthBearerExtensions)
			configMap.SetKey("sasl.oauthbearer.token.endpoint.url", *kafkaSASLOAuthBearerTokenEndpointURL)
		}
	}
	return producerConfigMap, consumerConfigMap
}

func determineAndSetGroupID(configMap *kafka.ConfigMap) {
	if *kafkaGroupIDAsPrefix {
		groupIDSuffix, err := uuid.NewUUID()
		if err != nil {
			log.Fatalln("Could not generate postfix UUID for group id")
		}
		eventualGroupID := *kafkaGroupID + "-" + groupIDSuffix.String()
		log.Println("Using group ID:", eventualGroupID)
		configMap.SetKey("group.id", eventualGroupID)
	} else {
		log.Println("Using group ID:", *kafkaGroupID)
		configMap.SetKey("group.id", *kafkaGroupID)
	}
}
