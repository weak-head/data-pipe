package pipeline

import (
	"context"
	"fmt"
	"testing"

	api "github.com/weak-head/data-pipe/api/v1"
	"github.com/weak-head/data-pipe/internal/logger"

	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestPipelineCreation(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		r *readerMock,
		w *writerMock,
		p *processorMock,
		s *sleeperMock,
		m *reporterMock,
		l logger.Log,
	){
		"fails to create if no reader provided":    testFailsIfNoReader,
		"fails to create if no writer provided":    testFailsIfNoWriter,
		"fails to create if no sleeper provided":   testFailsIfNoSleeper,
		"fails to create if no processor provided": testFailsIfNoProcessor,
		"fails to create if no reporter provided":  testFailsIfNoReporter,
	} {
		t.Run(scenario, func(t *testing.T) {
			reader := &readerMock{
				fetchResult: struct {
					kafka.Message
					error
				}{
					Message: kafka.Message{},
					error:   nil,
				},
				commitResult: nil,
			}
			writer := &writerMock{}
			processor := &processorMock{}
			sleeper := &sleeperMock{}
			reporter := &reporterMock{}
			log, _ := logger.NewNullLogger()

			fn(t, reader, writer, processor, sleeper, reporter, log)
		})
	}
}

func TestPipelineFlow(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		r *readerMock,
		w *writerMock,
		p *processorMock,
		s *sleeperMock,
		l *logtest.Hook,
		pipeline *Pipeline,
	){
		"pipeline exits on canceled context":            testExitOnContext,
		"pipeline exits on cancel after full cycle":     testExitAfterCycle,
		"tracks errors of fetch message":                testTracksErrorOnFetch,
		"resets sleeper on successfull cycle":           testResetSleeperOnCycle,
		"pipeline exits on N consecutive fetch errors":  testExitOnFetchErrors,
		"pipeline exits on N consecutive write errors":  testExitOnWriteErrors,
		"pipeline exits on N consecutive commit errors": testExitOnCommitErrors,
	} {
		t.Run(scenario, func(t *testing.T) {
			reader := &readerMock{
				fetchResult: struct {
					kafka.Message
					error
				}{
					Message: kafka.Message{},
					error:   nil,
				},
				commitResult: nil,
			}
			writer := &writerMock{}
			processor := &processorMock{}
			sleeper := &sleeperMock{}
			reporter := &reporterMock{}
			logger, hook := logger.NewNullLogger()

			pipeline, err := NewPipeline(
				reader,
				writer,
				processor,
				sleeper,
				reporter,
				logger,
			)
			require.NoError(t, err)

			fn(t, reader, writer, processor, sleeper, hook, pipeline)
		})
	}
}

type readerMock struct {
	fetchCount  int
	fetchHook   func()
	fetchResult struct {
		kafka.Message
		error
	}

	commitCount  int
	commitHook   func(msgs ...kafka.Message)
	commitResult error
}

type writerMock struct {
	writeCount  int
	writeHook   func(msgs ...kafka.Message)
	writeResult error
}

type processorMock struct {
}

type sleeperMock struct {
	sleepCount int
	resetCount int
}

type reporterMock struct {
}

func (r *readerMock) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.fetchCount++
	if r.fetchHook != nil {
		r.fetchHook()
	}

	return r.fetchResult.Message, r.fetchResult.error
}

func (r *readerMock) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	r.commitCount++
	if r.commitHook != nil {
		r.commitHook(msgs...)
	}
	return r.commitResult
}

func (w *writerMock) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.writeCount += len(msgs)
	if w.writeHook != nil {
		w.writeHook(msgs...)
	}
	return w.writeResult
}

func (p *processorMock) Process(ctx context.Context, frame *api.InputFrame) (*api.ConvertedBlob, error) {
	return &api.ConvertedBlob{}, nil
}

func (s *sleeperMock) Sleep() {
	s.sleepCount++
}

func (s *sleeperMock) Reset() {
	s.resetCount++
}

func (r *reporterMock) DataFrameProcessed(processingKind string, milliseconds float64) {}
func (r *reporterMock) ConvertionFinished(processingKind string, milliseconds float64) {}
func (r *reporterMock) PipelineFailed(failure string) {}

func testExitOnContext(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	l *logtest.Hook,
	pipeline *Pipeline,
) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pipeline.Run(ctx)
	require.Nil(t, err)

	require.Equal(t, 0, r.fetchCount)
	require.Equal(t, 0, r.commitCount)
	require.Equal(t, 0, w.writeCount)

	require.Equal(t, 2, len(l.Entries))
	require.Equal(t, logrus.InfoLevel, l.LastEntry().Level)
	require.Equal(t, "Pipeline has been stopped.", l.LastEntry().Message)
}

func testExitAfterCycle(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	l *logtest.Hook,
	pipeline *Pipeline,
) {
	ctx, cancel := context.WithCancel(context.Background())
	r.commitHook = func(msgs ...kafka.Message) {
		cancel()
	}

	err := pipeline.Run(ctx)
	require.Nil(t, err)

	require.Equal(t, 1, r.fetchCount)
	require.Equal(t, 1, r.commitCount)
	require.Equal(t, 1, w.writeCount)

	require.Equal(t, logrus.InfoLevel, l.LastEntry().Level)
	require.Equal(t, "Pipeline has been stopped.", l.LastEntry().Message)
}

func testTracksErrorOnFetch(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	l *logtest.Hook,
	pipeline *Pipeline,
) {
	ctx, cancel := context.WithCancel(context.Background())
	r.fetchHook = func() {
		cancel()
	}

	r.fetchResult = struct {
		kafka.Message
		error
	}{
		Message: kafka.Message{},
		error:   fmt.Errorf("Invalid hostname"),
	}

	err := pipeline.Run(ctx)
	require.Nil(t, err)

	require.Equal(t, 1, r.fetchCount)
	require.Equal(t, 0, r.commitCount)
	require.Equal(t, 0, w.writeCount)

	require.Equal(t, logrus.ErrorLevel, l.Entries[len(l.Entries)-2].Level)
	require.Equal(t, "Failed to fetch a message from the kafka reader", l.Entries[len(l.Entries)-2].Message)
}

func testExitOnFetchErrors(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	l *logtest.Hook,
	pipeline *Pipeline,
) {
	ctx := context.Background()

	r.fetchResult = struct {
		kafka.Message
		error
	}{
		Message: kafka.Message{},
		error:   fmt.Errorf("Invalid hostname"),
	}

	err := pipeline.Run(ctx)
	require.Equal(t, r.fetchResult.error, err)

	require.Equal(t, retryFetchCount, r.fetchCount)
	require.Equal(t, retryFetchCount-1, s.sleepCount)
	require.Equal(t, 0, r.commitCount)
	require.Equal(t, 0, w.writeCount)

	require.Equal(t, logrus.ErrorLevel, l.LastEntry().Level)
	require.Equal(
		t,
		fmt.Sprintf(
			"Giving up fetching the message. Stopping pipeline because of %d consecutive failed fetches",
			retryFetchCount),
		l.LastEntry().Message)
}

func testResetSleeperOnCycle(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	l *logtest.Hook,
	pipeline *Pipeline,
) {
	ctx, cancel := context.WithCancel(context.Background())
	r.commitHook = func(msgs ...kafka.Message) {
		cancel()
	}

	err := pipeline.Run(ctx)
	require.Nil(t, err)

	require.Equal(t, 1, r.fetchCount)
	require.Equal(t, 1, r.commitCount)
	require.Equal(t, 1, w.writeCount)
	require.Equal(t, 0, s.sleepCount)
	require.Equal(t, 1, s.resetCount)
}

func testExitOnWriteErrors(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	l *logtest.Hook,
	pipeline *Pipeline,
) {
	ctx := context.Background()

	w.writeResult = fmt.Errorf("Invalid hostname")

	err := pipeline.Run(ctx)
	require.Equal(t, w.writeResult, err)

	require.Equal(t, 1, r.fetchCount)
	require.Equal(t, retryWriteCount, w.writeCount)
	require.Equal(t, retryWriteCount-1, s.sleepCount)
	require.Equal(t, 0, r.commitCount)

	require.Equal(t, logrus.ErrorLevel, l.LastEntry().Level)
	require.Equal(
		t,
		fmt.Sprintf(
			"Giving up writing the message. Stopping pipeline because of %d consecutive failed writes",
			retryWriteCount),
		l.LastEntry().Message)
}

func testExitOnCommitErrors(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	l *logtest.Hook,
	pipeline *Pipeline,
) {
	ctx := context.Background()

	r.commitResult = fmt.Errorf("Invalid hostname")

	err := pipeline.Run(ctx)
	require.Equal(t, r.commitResult, err)

	require.Equal(t, 1, r.fetchCount)
	require.Equal(t, 1, w.writeCount)
	require.Equal(t, retryCommitCount, r.commitCount)
	require.Equal(t, retryCommitCount-1, s.sleepCount)

	require.Equal(t, logrus.ErrorLevel, l.LastEntry().Level)
	require.Equal(
		t,
		fmt.Sprintf(
			"Giving up committing the message. Stopping pipeline because of %d consecutive failed commits",
			retryWriteCount),
		l.LastEntry().Message)
}

func testFailsIfNoReader(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	m *reporterMock,
	l logger.Log,
) {
	pipeline, err := NewPipeline(
		nil,
		w,
		p,
		s,
		m,
		l,
	)
	require.Nil(t, pipeline)
	require.Equal(t, ErrNoReaderProvided, err)
}

func testFailsIfNoWriter(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	m *reporterMock,
	l logger.Log,
) {
	pipeline, err := NewPipeline(
		r,
		nil,
		p,
		s,
		m,
		l,
	)
	require.Nil(t, pipeline)
	require.Equal(t, ErrNoWriterProvided, err)
}

func testFailsIfNoProcessor(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	m *reporterMock,
	l logger.Log,
) {
	pipeline, err := NewPipeline(
		r,
		w,
		nil,
		s,
		m,
		l,
	)
	require.Nil(t, pipeline)
	require.Equal(t, ErrNoProcessorProvided, err)
}

func testFailsIfNoSleeper(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	m *reporterMock,
	l logger.Log,
) {
	pipeline, err := NewPipeline(
		r,
		w,
		p,
		nil,
		m,
		l,
	)
	require.Nil(t, pipeline)
	require.Equal(t, ErrNoSleeperProvided, err)
}

func testFailsIfNoReporter(
	t *testing.T,
	r *readerMock,
	w *writerMock,
	p *processorMock,
	s *sleeperMock,
	m *reporterMock,
	l logger.Log,
) {
	pipeline, err := NewPipeline(
		r,
		w,
		p,
		s,
		nil,
		l,
	)
	require.Nil(t, pipeline)
	require.Equal(t, ErrNoReporterProvided, err)
}
