package processor

import (
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	api "github.com/weak-head/data-pipe/api/v1"
	"github.com/weak-head/data-pipe/internal/logger"
)

func TestProcessorCreation(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		e *converterMock,
		s *storageMock,
		l logger.Log,
		h *logtest.Hook,
	){
		"fails to create if no converter is provided": testFailsIfNoConverter,
		"fails to create if no storage is provided":   testFailsIfNoStorage,
	} {
		t.Run(scenario, func(t *testing.T) {
			converter := &converterMock{
				err: nil,
			}
			storage := &storageMock{
				retrieveErr: nil,
				storeErr:    nil,
			}
			log, hook := logger.NewNullLogger()

			fn(t, converter, storage, log, hook)
		})
	}
}

func TestProcessorFlow(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		e *converterMock,
		s *storageMock,
		h *logtest.Hook,
		config *ProcessorConfig,
		frame *api.InputFrame,
		p *processor,
	){
		"fails to process the page if retrieval fails":  testFailsOnRetrievalError,
		"fails to process the page if convertion fails": testFailsOnConvertionError,
		"fails to process the page if storage fails":    testFailsOnStorageError,
		"process keeps the original frame info": testKeepsOriginalInfo,
	} {
		t.Run(scenario, func(t *testing.T) {
			converter := &converterMock{
				err: nil,
			}
			storage := &storageMock{
				retrieveErr: nil,
				storeErr:    nil,
			}
			log, hook := logger.NewNullLogger()

			config := ProcessorConfig{
				DestinationBucket: "destination_bucket",
			}

			processor, err := NewProcessor(
				config,
				converter,
				storage,
				log,
			)
			require.NotNil(t, processor)
			require.Nil(t, err)

			page := &api.InputFrame{
				FrameId: "frame_1",
				FrameLocation: &api.Location{
					Kind:       api.Location_MINIO,
					Bucket:     "bucket_11",
					ObjectName: "doc11",
				},
			}

			fn(t, converter, storage, hook, &config, page, processor)
		})
	}
}

type converterMock struct {
	err error
}

func (e *converterMock) Convert(ctx context.Context, page []byte) ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	return nil, nil
}

type storageMock struct {
	retrieveErr error
	storeErr    error
}

func (s *storageMock) Store(ctx context.Context, bucket string, objectName string, objectBytes []byte, contentType string) error {
	if s.storeErr != nil {
		return s.storeErr
	}
	return nil
}

func (s *storageMock) Retrieve(ctx context.Context, bucket string, objectName string) ([]byte, error) {
	if s.retrieveErr != nil {
		return nil, s.retrieveErr
	}
	return nil, nil
}

func testFailsIfNoConverter(
	t *testing.T,
	e *converterMock,
	s *storageMock,
	l logger.Log,
	h *logtest.Hook,
) {
	processor, err := NewProcessor(
		ProcessorConfig{},
		nil,
		s,
		l,
	)
	require.Nil(t, processor)
	require.Equal(t, ErrNoConverterProvided, err)
}

func testFailsIfNoStorage(
	t *testing.T,
	e *converterMock,
	s *storageMock,
	l logger.Log,
	h *logtest.Hook,
) {
	processor, err := NewProcessor(
		ProcessorConfig{},
		e,
		nil,
		l,
	)
	require.Nil(t, processor)
	require.Equal(t, ErrNoStorageProvided, err)
}

func testFailsOnRetrievalError(
	t *testing.T,
	e *converterMock,
	s *storageMock,
	h *logtest.Hook,
	config *ProcessorConfig,
	frame *api.InputFrame,
	p *processor,
) {
	s.retrieveErr = errors.New("connection failed")

	processedFrame, err := p.Process(context.Background(), frame)

	require.Nil(t, processedFrame)
	require.Equal(t, s.retrieveErr, err)

	require.Equal(t, logrus.ErrorLevel, h.LastEntry().Level)
	require.Equal(t, "Failed to retrive the data frame from the storage.", h.LastEntry().Message)
}

func testFailsOnConvertionError(
	t *testing.T,
	e *converterMock,
	s *storageMock,
	h *logtest.Hook,
	config *ProcessorConfig,
	frame *api.InputFrame,
	p *processor,
) {
	e.err = errors.New("failed to extract")

	processedFrame, err := p.Process(context.Background(), frame)

	require.Nil(t, processedFrame)
	require.Equal(t, e.err, err)

	require.Equal(t, logrus.ErrorLevel, h.LastEntry().Level)
	require.Equal(t, "Failed to convert data frame.", h.LastEntry().Message)
}

func testFailsOnStorageError(
	t *testing.T,
	e *converterMock,
	s *storageMock,
	h *logtest.Hook,
	config *ProcessorConfig,
	frame *api.InputFrame,
	p *processor,
) {
	s.storeErr = errors.New("connection failed")

	processedPage, err := p.Process(context.Background(), frame)

	require.Nil(t, processedPage)
	require.Equal(t, s.storeErr, err)

	require.Equal(t, logrus.ErrorLevel, h.LastEntry().Level)
	require.Equal(t, "Failed to store the converted data frame.", h.LastEntry().Message)
}

func testKeepsOriginalInfo(
	t *testing.T,
	e *converterMock,
	s *storageMock,
	h *logtest.Hook,
	config *ProcessorConfig,
	frame *api.InputFrame,
	p *processor,
) {
	converted, err := p.Process(context.Background(), frame)
	require.Nil(t, err)
	require.NotNil(t, converted)

	require.Equal(t, frame.FrameId, converted.FrameId)
	require.Equal(t, frame.FrameLocation.Kind, converted.FrameLocation.Kind)
	require.Equal(t, frame.FrameLocation.Bucket, converted.FrameLocation.Bucket)
	require.Equal(t, frame.FrameLocation.ObjectName, converted.FrameLocation.ObjectName)

	require.Equal(t, config.DestinationBucket, converted.ConvertedLocation.Bucket)
	require.Equal(t, getBlobObjectName(frame), converted.ConvertedLocation.ObjectName)
}
