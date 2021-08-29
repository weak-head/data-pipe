package storage

// StorageConfig
type StorageConfig struct {
	Endpoint  string
	UseSSL    bool
	AccessKey string
	SecretKey string

	Region                 string
	CreateBucketIfNotExist bool
}
