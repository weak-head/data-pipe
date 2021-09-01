package pipeline

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	kafka "github.com/segmentio/kafka-go"

	api "github.com/weak-head/data-pipe/api/v1"
	"github.com/weak-head/data-pipe/internal/logger"
)

const (
	// retryFetchCount defines the number of retries
	// to fetch a message from the reader before giving up.
	retryFetchCount = 3

	// retryWriteCount defines the number of retries
	// to write a message to the writer before giving up.
	retryWriteCount = 3

	// retryCommitCount defines the number of retries
	// to commit a message to the reader before giving up.
	retryCommitCount = 3

	// maxPipelines defines the maximum number of pipelines that
	// could be started in a single instance of the service
	maxPipelines = 10000
)

var (
	// ErrNoReaderProvided happens when reader is not provided.
	ErrNoReaderProvided = errors.New("no reader provided")

	// ErrNoWriterProvided happens when writer is not provided.
	ErrNoWriterProvided = errors.New("no writer provided")

	// ErrNoSleeperProvided happens when sleeper is not provided.
	ErrNoSleeperProvided = errors.New("no sleeper provided")

	// ErrNoProcessorProvided happens when processor is not provided.
	ErrNoProcessorProvided = errors.New("no processor provided")

	// ErrNoReporterProvided happens when reporter is not provided.
	ErrNoReporterProvided = errors.New("no reporter provided")

	// Unique pipeline ids
	uniqueIds chan string
)

// Generate unique IDs for the newly created pipelines
func init() {
	uniqueIds = make(chan string)
	go func(numInts int, ch chan string) {
		source := rand.NewSource(time.Now().UnixNano())
		generator := rand.New(source)
		for i := 0; i < numInts; i++ {
			ch <- fmt.Sprintf("p_%d", generator.Intn(numInts*100))
		}
	}(maxPipelines, uniqueIds)
}

// Reader is a transactional message reader.
type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Writer is an atomic message writer.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Processor defines a data frame processor.
type Processor interface {
	Process(ctx context.Context, frame *api.InputFrame) (*api.ConvertedBlob, error)
}

// Sleeper is a routine sleeper with some sleeping strategy
// and ability to reset the strategy state.
type Sleeper interface {
	Sleep()
	Reset()
}

// Reporter is a pipeline status and progress reporter that collects
// and aggregates metrics related to pipeline flow.
type Reporter interface {
	DataFrameProcessed(processingKind string, milliseconds float64)
	ConvertionFinished(processingKind string, milliseconds float64)
	PipelineFailed(failure string)
}

// Pipeline is a document processing pipeline.
type Pipeline struct {
	processor Processor
	reader    Reader
	writer    Writer

	sleeper  Sleeper
	reporter Reporter

	log logger.Log
}

// NewPipeline creates and initializes a new document processing pipeline.
func NewPipeline(
	reader Reader,
	writer Writer,
	processor Processor,
	sleeper Sleeper,
	reporter Reporter,
	log logger.Log,
) (*Pipeline, error) {
	if reader == nil {
		return nil, ErrNoReaderProvided
	}

	if writer == nil {
		return nil, ErrNoWriterProvided
	}

	if processor == nil {
		return nil, ErrNoProcessorProvided
	}

	if sleeper == nil {
		return nil, ErrNoSleeperProvided
	}

	if reporter == nil {
		return nil, ErrNoReporterProvided
	}

	return &Pipeline{
		processor: processor,
		reader:    reader,
		writer:    writer,
		sleeper:   sleeper,
		reporter:  reporter,
		log: log.WithFields(logger.Fields{
			logger.FieldPackage: "pipeline",
			"pipeline_id":       <-uniqueIds,
		}),
	}, nil
}

// Run starts the document processing pipeline,
// that ensures that each document is processed at least once.
//
// The document metadata is extracted from the kafka stream and sent to the extractor,
// that retrieves the document from the storage and does OCR, text, table
// and form extraction. The extracted information is saved back to the storage
// and the processed document metadata is send down the data pipeline
// to the specified kafka stream.
func (p *Pipeline) Run(ctx context.Context) error {
	log := p.log.WithField(logger.FieldFunction, "Pipeline.Run")
	log.Info("Starting the pipeline.")

	failedFetches := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("Pipeline has been stopped.")
			return nil
		default:
			// Nop
		}

		log.Info("Fetching the next message from the reader.")
		m, err := p.reader.FetchMessage(ctx)
		if err != nil {
			log.Error(err, "Failed to fetch a message from the kafka reader")

			failedFetches += 1
			if failedFetches >= retryFetchCount {
				log.Errorf(err,
					"Giving up fetching the message. Stopping pipeline because of %d consecutive failed fetches",
					retryFetchCount)
				return err
			} else {
				p.sleeper.Sleep()
				continue
			}
		}
		failedFetches = 0
		log.Info("Fetched a new message")

		frame := &api.InputFrame{}
		if err := frame.Unmarshal(m.Value); err != nil {
			// TODO: Drop the data frame and move on
			// TODO: Reflect the failure in the stream?
			continue
		}

		converted_blob, err := p.processor.Process(ctx, frame)
		if err != nil {
			// TODO: handle different reasons of failure
			continue
		}

		bytes, err := converted_blob.Marshal()
		if err != nil {
			// TODO: Drop the data frame and move on
			// TODO: Reflect the failure in the stream?
			continue
		}

		msg := kafka.Message{
			Key:   []byte(converted_blob.FrameId),
			Value: bytes,
		}

		messageWritten := false
		writeAttempt := 0
		for !messageWritten {
			if err := p.writer.WriteMessages(ctx, msg); err != nil {
				log.Error(err, "Failed to write the message to the kafka writer")

				writeAttempt += 1
				if writeAttempt >= retryWriteCount {
					log.Errorf(err,
						"Giving up writing the message. Stopping pipeline because of %d consecutive failed writes",
						retryWriteCount)
					return err
				} else {
					p.sleeper.Sleep()
					continue
				}
			}
			messageWritten = true
		}

		readCommitted := false
		commitAttempt := 0
		for !readCommitted {
			if err := p.reader.CommitMessages(ctx, m); err != nil {
				log.Error(err, "Failed to commit read message to the kafka reader")

				commitAttempt += 1
				if commitAttempt >= retryCommitCount {
					log.Errorf(err,
						"Giving up committing the message. Stopping pipeline because of %d consecutive failed commits",
						retryCommitCount)
					return err
				} else {
					p.sleeper.Sleep()
					continue
				}
			}
			readCommitted = true
		}

		p.sleeper.Reset()
	}
}
