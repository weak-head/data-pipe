package stream

// TopicConfig
type TopicConfig struct {
	Topic             string
	CreateIfNotExist  bool
	NumPartitions     int
	ReplicationFactor int
}

// ReaderConfig
type ReaderConfig struct {
	TopicConfig

	Brokers  []string
	GroupID  string
	MinBytes int
	MaxBytes int
}

// WriterConfig
type WriterConfig struct {
	TopicConfig

	Addr     string
	Balancer string
}
