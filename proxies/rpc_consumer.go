package proxies

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/santidhammo/dkr/messages"
	"log"
	"time"
)

/*
RPCConsumer consumes events from an RPC kafka topic, en convert them into messages.
*/
type RPCConsumer struct {
	kafkaConfig    *kafka.ConfigMap
	topic          string
	messageHandler func(*messages.Message)
	isClosedFn     func() bool
}

/*
NewRPCConsumer creates a new RPCConsumer.

An RPCConsumer is created using a kafka configuration map, a topic name a message handler handling
converted messages from events and possibly a function indicating to close the consumer.
*/
func NewRPCConsumer(
	kafkaConfig *kafka.ConfigMap,
	topic string,
	messageHandler func(message *messages.Message),
	isClosedFn func() bool) RPCConsumer {
	return RPCConsumer{
		kafkaConfig,
		topic,
		messageHandler,
		isClosedFn,
	}
}

/*
Start creates a Kafka consumer and starts polling the consumer.

The events received from the consumer are send to the messageHandler function until either the isClosedFn
function returns true, or the process is exited.
*/
func (c *RPCConsumer) Start() {
	first := true
retry:
	for {
		if !first {
			log.Println("Retrying consumption on topic:", c.topic)
			time.Sleep(time.Second)
		} else {
			first = false
		}
		consumer, err := kafka.NewConsumer(c.kafkaConfig)

		if err != nil {
			log.Println("Could not properly create consumer, error:", err.Error())
			continue retry
		}
		err = consumer.Subscribe(c.topic, nil)
		if err != nil {
			log.Println("Could not properly subscribe to topic:", c.topic, "error:", err.Error())
			continue retry
		}
		for {
			event := consumer.Poll(10)
			if c.isClosedFn != nil && c.isClosedFn() {
				log.Println("Closing RPC consumer")
				err := consumer.Close()
				if err != nil {
					log.Println("Could not close RPC consumer:", err.Error())
				}
				return
			}
			switch e := event.(type) {
			case *kafka.Message:
				message, err := messages.Decode(e.Value)
				if err != nil {
					log.Println("Error:", err.Error())
				} else {
					// Handling messages can take a long time, and in certain cases might not properly return until
					// a watchdog intervenes
					go c.messageHandler(message)
					_, err = consumer.Commit()
					if err != nil {
						log.Println("Could not commit to RPC consumer:", err.Error())
					}
				}
			case kafka.Error:
				log.Println("Error:", e)
				continue retry
			default:
				if e != nil {
					log.Println("Ignored Event:", e)
				}
			}
		}
	}
}
