package processor

import (
	"context"
	"errors"
	"fmt"

	api "github.com/weak-head/data-pipe/api/v1"
	"github.com/weak-head/data-pipe/internal/logger"
	"github.com/weak-head/data-pipe/internal/storage"
)

var (
	// ErrNoConverterProvided happens when converter is not provided.
	ErrNoConverterProvided = errors.New("no converter provided")

	// ErrNoStorageProvided happens when storage is not provided.
	ErrNoStorageProvided = errors.New("no storage provided")
)

const (
	contentTypeBLOB = "application/octet-stream"
)

// ConverterConfig
type ConverterConfig struct {
	Kind string
}

// ProcessorConfig
type ProcessorConfig struct {
	DestinationBucket string
}

// Config
type Config struct {
	Processor ProcessorConfig
	Converter ConverterConfig
	Storage   storage.StorageConfig
}

// Converter is the interface that wraps the basic Convert method.
//
// Convert process the data frame and returns the converted blob.
// Convert must return a non-nil error if convertion of the data frame has failed.
type Converter interface {
	Convert(ctx context.Context, from []byte) (to []byte, err error)
}

// Storage
type Storage interface {
	Store(ctx context.Context, bucket string, objectName string, objectBytes []byte, contentType string) error
	Retrieve(ctx context.Context, bucket string, objectName string) ([]byte, error)
}

// processor is a wrapper over the converter that interacts
// with the provided storage to retrive data frames and store
// the converted blob.
type processor struct {
	config ProcessorConfig

	converter Converter
	storage   Storage

	log logger.Log
}

// NewProcessor creates a new data frame processor.
// It returns an error if the creation failed.
func NewProcessor(
	config ProcessorConfig,
	converter Converter,
	storage Storage,
	log logger.Log,
) (*processor, error) {
	if converter == nil {
		return nil, ErrNoConverterProvided
	}

	if storage == nil {
		return nil, ErrNoStorageProvided
	}

	return &processor{
		config:    config,
		converter: converter,
		storage:   storage,
		log:       log.WithField(logger.FieldPackage, "processor"),
	}, nil
}

// Process retrieves the data frame from the storage,
// converts using the given converter and uploads results back the the storage.
// Process returns an error in case if the data frame convertion has failed.
func (p *processor) Process(ctx context.Context, frame *api.InputFrame) (*api.ConvertedBlob, error) {
	log := p.log.WithFields(logger.Fields{
		logger.FieldFunction: "processor.Process",
		"frame":           frame.FrameId,
	})
	log.Info("Processing a new data frame.")

	frame_bytes, err := p.storage.Retrieve(ctx, frame.FrameLocation.Bucket, frame.FrameLocation.ObjectName)
	if err != nil {
		log.Error(err, "Failed to retrive the data frame from the storage.")
		return nil, err
	}

	blob_bytes, err := p.converter.Convert(ctx, frame_bytes)
	if err != nil {
		log.Error(err, "Failed to convert data frame.")
		return nil, err
	}

	objectName := getBlobObjectName(frame)
	if err := p.storage.Store(ctx, p.config.DestinationBucket, objectName, blob_bytes, contentTypeBLOB); err != nil {
		log.Error(err, "Failed to store the converted data frame.")
		return nil, err
	}

	log.Info("Data frame has been processed.")
	return &api.ConvertedBlob{
		FrameId: frame.FrameId,
		FrameLocation: &api.Location{
			Kind:       frame.FrameLocation.Kind,
			Bucket:     frame.FrameLocation.Bucket,
			ObjectName: frame.FrameLocation.ObjectName,
		},
		ConvertedLocation: &api.Location{
			Bucket:     p.config.DestinationBucket,
			ObjectName: objectName,
		},
	}, nil
}

func getBlobObjectName(frame *api.InputFrame) string {
	return fmt.Sprintf("converted_%s.blob", frame.FrameId)
}
