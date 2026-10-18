package main

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type VMWriter struct {
	writeURL   string
	client     *http.Client
	queueChan  chan prompb.TimeSeries
	stopChan   chan struct{}
	flushBatch int
}

func NewVMWriter(writeURL string) *VMWriter {
	if writeURL == "" {
		return nil
	}
	w := &VMWriter{
		writeURL: writeURL,
		client: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        50,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		queueChan:  make(chan prompb.TimeSeries, 4000),
		stopChan:   make(chan struct{}),
		flushBatch: 100,
	}
	go w.startFlusher()
	return w
}

func (w *VMWriter) PushSeries(labels []prompb.Label, val float64, tsMs int64) {
	if w == nil {
		return
	}
	ts := prompb.TimeSeries{
		Labels:  labels,
		Samples: []prompb.Sample{{Value: val, Timestamp: tsMs}},
	}
	select {
	case w.queueChan <- ts:
	default:
	}
}

func (w *VMWriter) Close() {
	if w != nil {
		close(w.stopChan)
	}
}

func (w *VMWriter) startFlusher() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	batch := make([]prompb.TimeSeries, 0, w.flushBatch)

	for {
		select {
		case ts := <-w.queueChan:
			batch = append(batch, ts)
			if len(batch) >= w.flushBatch {
				w.sendBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				w.sendBatch(batch)
				batch = batch[:0]
			}
		case <-w.stopChan:
			if len(batch) > 0 {
				w.sendBatch(batch)
			}
			return
		}
	}
}

func (w *VMWriter) sendBatch(series []prompb.TimeSeries) {
	reqData := &prompb.WriteRequest{Timeseries: series}
	size := reqData.Size()

	bufPtr := byteBufferPool.Get().(*[]byte)
	defer func() {
		*bufPtr = (*bufPtr)[:0]
		byteBufferPool.Put(bufPtr)
	}()

	if cap(*bufPtr) < size {
		*bufPtr = make([]byte, size)
	} else {
		*bufPtr = (*bufPtr)[:size]
	}

	n, err := reqData.MarshalTo(*bufPtr)
	if err != nil {
		return
	}

	maxComp := snappy.MaxEncodedLen(n)
	compBufPtr := byteBufferPool.Get().(*[]byte)
	defer func() {
		*compBufPtr = (*compBufPtr)[:0]
		byteBufferPool.Put(compBufPtr)
	}()

	if cap(*compBufPtr) < maxComp {
		*compBufPtr = make([]byte, maxComp)
	} else {
		*compBufPtr = (*compBufPtr)[:maxComp]
	}

	compressed := snappy.Encode(*compBufPtr, (*bufPtr)[:n])

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.writeURL, bytes.NewReader(compressed))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, err := w.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}
