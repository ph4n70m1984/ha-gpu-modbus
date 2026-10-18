package main

import (
	"math"
	"sync"
	"time"
)

var floatSlicePool = sync.Pool{
	New: func() any {
		b := make([]float64, 0, 256)
		return &b
	},
}

var byteBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 64*1024)
		return &b
	},
}

type RingBuffer struct {
	mu       sync.RWMutex
	data     []float64
	capacity int
	head     int
	isFull   bool
}

func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 60
	}
	return &RingBuffer{
		data:     make([]float64, capacity),
		capacity: capacity,
	}
}

func (rb *RingBuffer) Push(val float64) {
	rb.mu.Lock()
	rb.data[rb.head] = val
	rb.head = (rb.head + 1) % rb.capacity
	if rb.head == 0 {
		rb.isFull = true
	}
	rb.mu.Unlock()
}

func (rb *RingBuffer) Reset() {
	rb.mu.Lock()
	rb.head = 0
	rb.isFull = false
	rb.mu.Unlock()
}

func (rb *RingBuffer) MinMax() (minVal, maxVal float64, count int) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.isFull {
		count = rb.capacity
	} else {
		count = rb.head
	}
	if count == 0 {
		return 0, 0, 0
	}

	minVal, maxVal = rb.data[0], rb.data[0]
	for i := 1; i < count; i++ {
		v := rb.data[i]
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	return minVal, maxVal, count
}

func (rb *RingBuffer) CopyTo(dst []float64) int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	var count int
	if rb.isFull {
		count = rb.capacity
	} else {
		count = rb.head
	}

	if len(dst) < count {
		count = len(dst)
	}

	if !rb.isFull {
		copy(dst, rb.data[:count])
		return count
	}

	n1 := copy(dst, rb.data[rb.head:])
	copy(dst[n1:], rb.data[:rb.head])
	return count
}

type TimedPoint struct {
	timestamp time.Time
	value     float64
}

type TrendBuffer struct {
	mu     sync.RWMutex
	points []TimedPoint
	maxAge time.Duration
}

func NewTrendBuffer(maxAge time.Duration) *TrendBuffer {
	return &TrendBuffer{
		points: make([]TimedPoint, 0, 1000),
		maxAge: maxAge,
	}
}

func (tb *TrendBuffer) Reset() {
	tb.mu.Lock()
	tb.points = tb.points[:0]
	tb.mu.Unlock()
}

func (tb *TrendBuffer) Push(val float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	if val <= 0 || val >= 150.0 {
		return
	}

	now := time.Now()
	tb.points = append(tb.points, TimedPoint{timestamp: now, value: val})

	cutoff := now.Add(-tb.maxAge)
	idx := 0
	for idx < len(tb.points) && tb.points[idx].timestamp.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		tb.points = append(tb.points[:0], tb.points[idx:]...)
	}
}

func (tb *TrendBuffer) CalculateRatePerHour() float64 {
	tb.mu.RLock()
	defer tb.mu.RUnlock()

	n := len(tb.points)
	if n < 30 {
		return 0.0
	}

	minVal, maxVal := tb.points[0].value, tb.points[0].value
	for _, p := range tb.points {
		if p.value < minVal {
			minVal = p.value
		}
		if p.value > maxVal {
			maxVal = p.value
		}
	}

	if (maxVal - minVal) < 1.9 {
		return 0.0
	}

	t0 := tb.points[0].timestamp
	var sumT, sumY, sumTY, sumT2 float64

	for _, p := range tb.points {
		tHours := p.timestamp.Sub(t0).Hours()
		y := p.value
		sumT += tHours
		sumY += y
		sumTY += tHours * y
		sumT2 += tHours * tHours
	}

	nf := float64(n)
	denom := nf*sumT2 - sumT*sumT
	if math.Abs(denom) < 1e-9 {
		return 0.0
	}

	slope := (nf*sumTY - sumT*sumY) / denom

	if slope < 8.0 {
		return 0.0
	}

	return slope
}
