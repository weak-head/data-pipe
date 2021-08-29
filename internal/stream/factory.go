package stream

import (
	"net"
	"strconv"

	kafka "github.com/segmentio/kafka-go"
)

// NewReader
func NewReader(config ReaderConfig) (*kafka.Reader, error) {
	if err := createTopic(config.Brokers[0], config.TopicConfig); err != nil {
		return nil, err
	}

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  config.Brokers,
		GroupID:  config.GroupID,
		Topic:    config.Topic,
		MinBytes: config.MinBytes,
		MaxBytes: config.MaxBytes,
	}), nil
}

// NewWriter
func NewWriter(config WriterConfig) (*kafka.Writer, error) {
	if err := createTopic(config.Addr, config.TopicConfig); err != nil {
		return nil, err
	}

	return &kafka.Writer{
		Addr:     kafka.TCP(config.Addr),
		Topic:    config.Topic,
		Balancer: createBalancer(config.Balancer),
	}, nil
}

// createTopic
func createTopic(addr string, config TopicConfig) error {
	if !config.CreateIfNotExist {
		return nil
	}

	// Connect to some node
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Get the current controller
	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	// Connect to the current controller
	controllerConn, err := kafka.Dial(
		"tcp",
		net.JoinHostPort(
			controller.Host,
			strconv.Itoa(controller.Port),
		),
	)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{{
		Topic:             config.Topic,
		NumPartitions:     config.NumPartitions,
		ReplicationFactor: config.ReplicationFactor,
	}}

	// Create the topic
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return err
	}

	return nil
}

// createBalancer
func createBalancer(balancer string) kafka.Balancer {
	switch balancer {

	// Classical round robin
	case "roundrobin":
		return &kafka.RoundRobin{}

	// Partition that received the least bytes
	case "leastbytes":
		return &kafka.LeastBytes{}

	// FNV-1a
	case "hash":
		return &kafka.Hash{}

	// CRC32 hash
	case "crc32":
		return &kafka.CRC32Balancer{}

	// Murmur2 hash
	case "murmur2":
		return &kafka.Murmur2Balancer{}

	default:
		return &kafka.LeastBytes{}
	}
}
