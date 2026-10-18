package main

import (
	"math"
	"strings"
	"sync"

	"github.com/prometheus/prometheus/prompb"
)

type AnomalyOutput struct {
	CurrentVal   float64
	Median       float64
	UpperBound   float64
	LowerBound   float64
	AnomalyScore float64
	IsReady      bool
}

type AnomalyRegistry struct {
	mu          sync.RWMutex
	buffers     map[string]*RingBuffer
	windowSize  int
	minPoints   int
	sensitivity float64
}

func NewAnomalyRegistry(windowSize, minPoints int, sensitivity float64) *AnomalyRegistry {
	if windowSize <= 0 {
		windowSize = 60
	}
	if minPoints <= 0 {
		minPoints = 15
	}
	if sensitivity <= 0 {
		sensitivity = 3.0
	}
	return &AnomalyRegistry{
		buffers:     make(map[string]*RingBuffer),
		windowSize:  windowSize,
		minPoints:   minPoints,
		sensitivity: sensitivity,
	}
}

func (r *AnomalyRegistry) getOrCreate(key string) *RingBuffer {
	r.mu.RLock()
	rb, exists := r.buffers[key]
	r.mu.RUnlock()
	if exists {
		return rb
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if rb, exists = r.buffers[key]; exists {
		return rb
	}
	rb = NewRingBuffer(r.windowSize)
	r.buffers[key] = rb
	return rb
}

func (r *AnomalyRegistry) IngestAndEvaluate(key string, val float64) AnomalyOutput {
	rb := r.getOrCreate(key)
	rb.Push(val)

	bufPtr := floatSlicePool.Get().(*[]float64)
	defer func() {
		*bufPtr = (*bufPtr)[:0]
		floatSlicePool.Put(bufPtr)
	}()

	if cap(*bufPtr) < r.windowSize {
		*bufPtr = make([]float64, r.windowSize)
	} else {
		*bufPtr = (*bufPtr)[:r.windowSize]
	}

	n := rb.CopyTo(*bufPtr)
	if n < r.minPoints {
		return AnomalyOutput{CurrentVal: val, IsReady: false}
	}

	window := (*bufPtr)[:n]
	median, mad := CalculateMedianAndMADFast(window)

	minMad := math.Abs(median) * 0.02
	if minMad < 0.5 {
		minMad = 0.5
	}
	if mad < minMad {
		mad = minMad
	}

	upperBound := median + r.sensitivity*mad
	lowerBound := median - r.sensitivity*mad
	anomalyScore := math.Abs(val-median) / (r.sensitivity * mad)

	if anomalyScore > 10.0 {
		anomalyScore = 10.0
	}

	return AnomalyOutput{
		CurrentVal:   val,
		Median:       median,
		UpperBound:   upperBound,
		LowerBound:   lowerBound,
		AnomalyScore: anomalyScore,
		IsReady:      true,
	}
}

func quickselect(a []float64, k int) float64 {
	left, right := 0, len(a)-1
	for left < right {
		pivot := a[left+(right-left)/2]
		lt, i, gt := left, left, right
		for i <= gt {
			if a[i] < pivot {
				a[lt], a[i] = a[i], a[lt]
				lt++
				i++
			} else if a[i] > pivot {
				a[i], a[gt] = a[gt], a[i]
				gt--
			} else {
				i++
			}
		}
		if k >= lt && k <= gt {
			return pivot
		} else if k < lt {
			right = lt - 1
		} else {
			left = gt + 1
		}
	}
	return a[left]
}

func findMedianInplace(a []float64) float64 {
	n := len(a)
	if n == 0 {
		return 0
	}
	mid := n / 2
	if n%2 == 1 {
		return quickselect(a, mid)
	}
	m1 := quickselect(a, mid-1)
	m2 := quickselect(a, mid)
	return (m1 + m2) * 0.5
}

func CalculateMedianAndMADFast(data []float64) (float64, float64) {
	n := len(data)
	if n == 0 {
		return 0, 0
	}

	bufPtr1 := floatSlicePool.Get().(*[]float64)
	bufPtr2 := floatSlicePool.Get().(*[]float64)
	defer func() {
		*bufPtr1 = (*bufPtr1)[:0]
		*bufPtr2 = (*bufPtr2)[:0]
		floatSlicePool.Put(bufPtr1)
		floatSlicePool.Put(bufPtr2)
	}()

	workBuf := append(*bufPtr1, data...)
	median := findMedianInplace(workBuf)

	devBuf := *bufPtr2
	for _, val := range data {
		devBuf = append(devBuf, math.Abs(val-median))
	}
	*bufPtr2 = devBuf

	madRaw := findMedianInplace(devBuf)
	mad := madRaw * 1.4826

	return median, mad
}

func isProcessMetric(reg RegisterConfig) bool {
	id := strings.ToLower(reg.ID)
	name := strings.ToLower(reg.Name)
	unit := strings.ToLower(reg.Unit)
	regType := strings.ToLower(reg.Type)

	if regType == "coil" ||
		strings.Contains(id, "accum_") ||
		strings.Contains(id, "mode_") ||
		strings.Contains(id, "running_status") ||
		strings.Contains(id, "fuel_level") ||
		strings.Contains(id, "motochasy") ||
		strings.Contains(id, "energy") ||
		strings.Contains(id, "vyrabotannaya") ||
		strings.Contains(id, "zapusk") ||
		strings.Contains(id, "minuty") ||
		strings.Contains(id, "sekundy") ||
		strings.Contains(id, "status") ||
		strings.Contains(id, "rezhim") ||
		strings.Contains(id, "lsb") ||
		strings.Contains(id, "msb") ||
		strings.Contains(name, "моточасы") ||
		strings.Contains(name, "энергия") ||
		strings.Contains(name, "запуск") ||
		strings.Contains(name, "наработка") ||
		strings.Contains(name, "секунды") ||
		strings.Contains(name, "статус") ||
		strings.Contains(name, "режим") ||
		strings.Contains(name, "топлив") ||
		strings.Contains(name, "lsb") ||
		strings.Contains(name, "msb") ||
		unit == "kwh" ||
		unit == "min" ||
		unit == "h" ||
		unit == "s" {
		return false
	}
	return true
}

func isSignedRegister(reg RegisterConfig) bool {
	if reg.Signed {
		return true
	}
	id := strings.ToLower(reg.ID)
	devClass := strings.ToLower(reg.DeviceClass)
	unit := strings.ToLower(reg.Unit)

	if devClass == "power" || devClass == "temperature" ||
		unit == "kw" || unit == "kvar" || unit == "kva" || unit == "°c" ||
		strings.Contains(id, "power") || strings.Contains(id, "temp") || strings.Contains(id, "cos") {
		return true
	}
	return false
}

func detectMetricRole(idLower string) MetricRole {
	switch {
	case idLower == "current_a" || strings.Contains(idLower, "tok_fazy_a"):
		return RoleCurrentA
	case idLower == "current_b" || strings.Contains(idLower, "tok_fazy_b"):
		return RoleCurrentB
	case idLower == "current_c" || strings.Contains(idLower, "tok_fazy_c"):
		return RoleCurrentC
	case strings.Contains(idLower, "oboroty") || strings.Contains(idLower, "rpm") || strings.Contains(idLower, "speed"):
		return RoleRPM
	case strings.Contains(idLower, "temperatura_okhlazhd") || strings.Contains(idLower, "coolant"):
		return RoleCoolantTemp
	default:
		return RoleNone
	}
}

func buildPromLabels(deviceID, genName, regID, metricName string) []prompb.Label {
	return []prompb.Label{
		{Name: "__name__", Value: metricName},
		{Name: "device_id", Value: deviceID},
		{Name: "generator", Value: genName},
		{Name: "register", Value: regID},
	}
}
