package storage

import (
	"bytes"
	"context"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/weak-head/data-pipe/internal/logger"
)

// StorageConfig
type StorageConfig struct {
	Endpoint  string
	UseSSL    bool
	AccessKey string
	SecretKey string

	Region                 string
	CreateBucketIfNotExist bool
}

// minioStorage
type minioStorage struct {
	config StorageConfig
	client *minio.Client

	log logger.Log
}

// NewMinioStorage
func NewMinioStorage(conf StorageConfig, log logger.Log) (*minioStorage, error) {
	l := log.WithFields(logger.Fields{
		logger.FieldPackage:  "storage",
		logger.FieldFunction: "NewMinioStorage",
	})

	minioClient, err := minio.New(conf.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(conf.AccessKey, conf.SecretKey, ""),
		Secure: conf.UseSSL,
	})
	if err != nil {
		l.Error(err, "Failed to create a new minio client.")
		return nil, err
	}

	l.Info("Created a new minio storage client.")

	return &minioStorage{
		config: conf,
		client: minioClient,
		log:    l,
	}, nil
}

// Store
func (m *minioStorage) Store(
	ctx context.Context,
	bucket string,
	objectName string,
	objectBytes []byte,
	contentType string,
) error {
	log := m.log.WithFields(logger.Fields{
		logger.FieldFunction: "minioStorage.Store",
		"bucket":             bucket,
		"objectName":         objectName,
	})

	if m.config.CreateBucketIfNotExist {
		if err := m.createBucket(ctx, bucket); err != nil {
			log.Error(err, "Failed to create a new bucket.")
			return err
		}
		log.Info("Created a new bucket.")
	}

	r := bytes.NewReader(objectBytes)
	_, err := m.client.PutObject(ctx, bucket, objectName, r, r.Size(), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		log.Error(err, "Failed to store the object.")
		return err
	}

	log.Info("Uploaded a new object to the storage.")
	return nil
}

// Retrive
func (m *minioStorage) Retrieve(
	ctx context.Context,
	bucket string,
	objectName string,
) ([]byte, error) {
	log := m.log.WithFields(logger.Fields{
		logger.FieldFunction: "minioStorage.Retrieve",
		"bucket":             bucket,
		"objectName":         objectName,
	})

	stream, err := m.client.GetObject(ctx, bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		log.Error(err, "Failed to retrieve the object stream from the storage.")
		return nil, err
	}

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(stream); err != nil {
		log.Error(err, "Failed to read the object from the storage.")
		return nil, err
	}

	log.Info("Retrieved the object from the storage.")
	return buf.Bytes(), nil
}

// createBucket
func (m *minioStorage) createBucket(ctx context.Context, bucket string) error {
	log := m.log.WithFields(logger.Fields{
		logger.FieldFunction: "minioStorage.createBucket",
		"bucket":             bucket,
	})

	exists, err := m.client.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}

	if exists {
		log.Trace("Bucket already exist.")
		return nil
	}

	if err := m.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: m.config.Region}); err != nil {
		return err
	}

	log.Info("A new bucket has been created.")
	return nil
}
