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
		kafkaConfigMapSetKeyOrFatal(configMap, "client.id", *kafkaClientId)
		kafkaConfigMapSetKeyOrFatal(configMap, "metadata.broker.list", *kafkaMetadataBrokerList)
		if kafkaDebug != nil && len(*kafkaDebug) > 0 {
			kafkaConfigMapSetKeyOrFatal(configMap, "debug", *kafkaDebug)
		}
		kafkaConfigMapSetKeyOrFatal(configMap, "socket.timeout.ms", int(*kafkaSocketTimeoutMs))
		kafkaConfigMapSetKeyOrFatal(configMap, "socket.keepalive.enable", *kafkaSocketKeepalive)
		kafkaConfigMapSetKeyOrFatal(configMap, "socket.nagle.disable", *kafkaSocketNoNagle)
		kafkaConfigMapSetKeyOrFatal(configMap, "broker.address.family", *kafkaBrokerAddressFamily)
		kafkaConfigMapSetKeyOrFatal(configMap, "log_level", int(*kafkaLogLevel))
		kafkaConfigMapSetKeyOrFatal(configMap, "security.protocol", *kafkaSecurityProtocol)
		kafkaConfigMapSetKeyOrFatal(configMap, "ssl.key.location", *kafkaPrivateKeyFile)
		kafkaConfigMapSetKeyOrFatal(configMap, "ssl.key.password", *kafkaPrivateKeyPassphrase)
		kafkaConfigMapSetKeyOrFatal(configMap, "ssl.certificate.location", *kafkaPublicKeyFile)
		kafkaConfigMapSetKeyOrFatal(configMap, "ssl.ca.location", *kafkaCAPath)
		kafkaConfigMapSetKeyOrFatal(configMap, "enable.ssl.certificate.verification", *kafkaVerifyCertificates)

		if configMap == consumerConfigMap {
			determineAndSetGroupID(configMap)
			kafkaConfigMapSetKeyOrFatal(configMap, "group.instance.id", *kafkaGroupInstanceID)
			// We don't want old stuff when starting the consumer
			kafkaConfigMapSetKeyOrFatal(configMap, "auto.offset.reset", "latest")
		}
		if configMap == producerConfigMap {
			kafkaConfigMapSetKeyOrFatal(configMap, "compression.codec", *kafkaCompressionCodec)
		}

		if kafkaSecurityProtocol != nil && strings.HasPrefix(*kafkaSecurityProtocol, "SASL_") {
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.mechanism", *kafkaSASLMechanism)
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.kerberos.service.name", *kafkaSASLKerberosServiceName)
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.kerberos.principal", *kafkaSASLKerberosPrincipal)
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.kerberos.keytab", *kafkaSASLKerberosKeytabFile)
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.username", *kafkaSASLUsername)
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.password", *kafkaSASLPassword)
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.oauthbearer.config", *kafkaSASLOAuthBearerConfig)
			kafkaConfigMapSetKeyOrFatal(configMap, "enable.sasl.oauthbearer.unsecure.jwt", *kafkaSASLOAuthBearerUnsecureJWT)
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.oauthbearer.method", *kafkaSASLOAuthBearerMethod)
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.oauthbearer.client.id", *kafkaSASLOAuthBearerClientID)
			if kafkaSASLOAuthBearerClientSecret != nil {
				kafkaConfigMapSetKeyOrFatal(configMap, "sasl.oauthbearer.client.secret", *kafkaSASLOAuthBearerClientSecret)
			}
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.oauthbearer.scope", *kafkaSASLOAuthBearerScope)
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.oauthbearer.extensions", *kafkaSASLOAuthBearerExtensions)
			kafkaConfigMapSetKeyOrFatal(configMap, "sasl.oauthbearer.token.endpoint.url", *kafkaSASLOAuthBearerTokenEndpointURL)
		}
	}
	return producerConfigMap, consumerConfigMap
}

func kafkaConfigMapSetKeyOrFatal(m *kafka.ConfigMap, key string, value kafka.ConfigValue) {
	err := m.SetKey(key, value)
	if err != nil {
		log.Fatalln("Could not set Kafka configuration map key:", key, "to value:", value, "error:", err.Error())
	}
}

func determineAndSetGroupID(configMap *kafka.ConfigMap) {
	if *kafkaGroupIDAsPrefix {
		groupIDSuffix, err := uuid.NewUUID()
		if err != nil {
			log.Fatalln("Could not generate postfix UUID for group id")
		}
		eventualGroupID := *kafkaGroupID + "-" + groupIDSuffix.String()
		log.Println("Using group ID:", eventualGroupID)
		kafkaConfigMapSetKeyOrFatal(configMap, "group.id", eventualGroupID)
	} else {
		log.Println("Using group ID:", *kafkaGroupID)
		kafkaConfigMapSetKeyOrFatal(configMap, "group.id", *kafkaGroupID)
	}
}
