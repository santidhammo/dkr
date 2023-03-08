package proxies

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/santidhammo/dkr/messages"
	"log"
	"sync"
	"time"
)

/*
MessageConsumer consumes events from an RPC kafka topic, en convert them into messages.
*/
type MessageConsumer struct {
	kafkaConfig    *kafka.ConfigMap
	topic          string
	messageHandler func(messages.Message)
	closeWg        *sync.WaitGroup
}

/*
NewMessageConsumer creates a new MessageConsumer.

An MessageConsumer is created using a kafka configuration map, a topic name a message handler handling
converted messages from events and possibly a function indicating to close the consumer.
*/
func NewMessageConsumer(
	kafkaConfig *kafka.ConfigMap,
	topic string,
	messageHandler func(message messages.Message)) *MessageConsumer {
	return &MessageConsumer{
		kafkaConfig,
		topic,
		messageHandler,
		nil,
	}
}

/*
Start creates a Kafka consumer and starts polling the consumer.

The events received from the consumer are send to the messageHandler function until either the isClosedFn
function returns true, or the process is exited.
*/
func (c *MessageConsumer) Start() {
	log.Println("Starting RPC consumer for:", c.topic)
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
			if c.closeWg != nil {
				log.Println("Closing RPC consumer for:", c.topic)
				err := consumer.Close()
				if err != nil {
					log.Println("Could not close RPC consumer:", err.Error())
				}
				log.Println("Closed RPC consumer for:", c.topic)
				c.closeWg.Done()
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

func (c *MessageConsumer) Close() {
	newCloseWg := &sync.WaitGroup{}
	newCloseWg.Add(1)
	// Note, do not assign to c.closeWg earlier, otherwise it will end up in a deadlock
	c.closeWg = newCloseWg
	newCloseWg.Wait()
	c.closeWg = nil
}
