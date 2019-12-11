package rabbit

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
)

// AMQPClient contains the basic objects for a AMQ connection
type AMQPClient struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Reception  struct {
		Queue amqp.Queue
	}
	Dispatch struct{}
}

// AMQPClient_GetQueueName return the queue name for a receiver
func (a *AMQPClient) GetQueueName() string {
	return a.Reception.Queue.Name
}

// AMQPClient_StartConnection Starts the connection with rabbitMQ server.
// Dials up and creates a channel
func (a *AMQPClient) StartConnection(username, password, host string, port int) error {

	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", username, password, host, port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("amqp dial failure: %s", err)
	}
	a.Connection = conn
	errCh := a.CreateChannel()
	if errCh != nil {
		return fmt.Errorf("couldn't create a channel in start connection err: %s", errCh)
	}
	return nil
}

// AMQPClient_CreateChannel creates a channel and saves it in struct
func (a *AMQPClient) CreateChannel() error {
	ch, err := a.Connection.Channel()
	if err != nil {
		return fmt.Errorf("channel creating failure: %s", err)
	}
	a.Channel = ch
	return nil
}

// AMQPClient_SetupQueues Declares and binds a queue to an exchange
func (a *AMQPClient) SetupQueues(queueName string, queueIsDurable, autoDelete bool, routingKeys []string, exchange string) error {
	q, err := a.Channel.QueueDeclare(
		queueName,      // name
		queueIsDurable, // durable
		autoDelete,     // delete when un used
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue with name: %s, err: %s", queueName, err)
	}
	for _, key := range routingKeys {
		errBind := a.Channel.QueueBind(
			queueName,
			key,
			exchange,
			false,
			nil,
		)
		if errBind != nil {
			return fmt.Errorf("failed to Bind a queue err: %s", errBind)
		}
	}
	a.Reception.Queue = q
	return nil
}

// AMQPClient_StartReceiver Starts a rabbit MQ receiver with the passed configuration, returns a channel
// that will receive the messages, along with the connection and channel instance
func (a *AMQPClient) StartReceiver(queueName string, isDurable, autoDelete bool, routingKeys []string, exchanges interface{}) (<-chan amqp.Delivery, error) {
	var tag string
	switch exchanges.(type){
	case []string:
		for _, exchange := range exchanges.([]string) {
			err := a.SetupQueues(
				queueName, isDurable, autoDelete, routingKeys, exchange,
			)
			if err != nil {
				return make(chan amqp.Delivery), fmt.Errorf("failed to setup queue err: %s", err)
			}
		}
		tag = exchanges.([]string)[0]
	case string:
		err := a.SetupQueues(
			queueName, isDurable, autoDelete, routingKeys, exchanges.(string),
		)
		if err != nil {
			return make(chan amqp.Delivery), fmt.Errorf("failed to setup queue err: %s", err)
		}
		tag = exchanges.(string)

	default:
		return make(chan amqp.Delivery), fmt.Errorf("failed to declare exchange due to wrong type in var %v", exchanges)
	}
	messages, err := a.Channel.Consume(
		queueName, // queue
		tag,  // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return make(chan amqp.Delivery), fmt.Errorf("failed to register a consumer: %s", err)
	}
	return messages, nil
}

// AMQPClient_SetupDispatcher Declares the exchanges to be used to deliver messages
func (a *AMQPClient) SetupDispatcher(exchange, exchangeType string, isDurable, autoDelete bool) error {
	if err := a.Channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		isDurable,    // durable
		autoDelete,   // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return fmt.Errorf("exchange declare: %s", err)
	}
	return nil
}

// AMQPClient_SendMessage Deliver the message to the specified exchange, if exchange not created this will
// throw an error
func (a *AMQPClient) SendMessage(exchange, routingKey string, message interface{}) error {
	messageBody, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("couldn't marshal the map to an slice of bytes")
	}
	if err = a.Channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            messageBody,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
		},
	); err != nil {
		return fmt.Errorf("exchange publish: %s", err)
	}
	return nil
}
