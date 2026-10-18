package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/simonvetter/modbus"
)

// ==========================================
// КОНФИГУРАЦИОННЫЕ МОДЕЛИ
// ==========================================

type CommandConfig struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	Address uint16 `json:"address"`
	Type    string `json:"type"`
	Value   uint16 `json:"value"`
}

type ControllerModel struct {
	ModelID      string           `json:"model_id"`
	Name         string           `json:"name"`
	Manufacturer string           `json:"manufacturer"`
	Registers    []RegisterConfig `json:"registers"`
	Commands     []CommandConfig  `json:"commands,omitempty"`
}

type RegisterConfig struct {
	Name        string  `json:"name"`
	ID          string  `json:"id"`
	Address     uint16  `json:"address"`
	Type        string  `json:"type"`
	Bitrate     int     `json:"bitrate"`
	Signed      bool    `json:"signed,omitempty"`
	Unit        string  `json:"unit"`
	DeviceClass string  `json:"device_class"`
	Scale       float64 `json:"scale"`
}

type UserDevice struct {
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	Address            string  `json:"address"`
	ModelID            string  `json:"model_id"`
	TimeoutSec         int     `json:"timeout_sec"`
	AnomalyWindow      int     `json:"anomaly_window,omitempty"`
	AnomalySensitivity float64 `json:"anomaly_sensitivity,omitempty"`
}

type HADiscoveryPayload struct {
	Name              string         `json:"name"`
	StateTopic        string         `json:"state_topic"`
	UnitOfMeasurement string         `json:"unit_of_measurement,omitempty"`
	DeviceClass       string         `json:"device_class,omitempty"`
	UniqueID          string         `json:"unique_id"`
	Device            map[string]any `json:"device"`
}

type HAButtonDiscovery struct {
	Name         string         `json:"name"`
	CommandTopic string         `json:"command_topic"`
	PayloadPress string         `json:"payload_press"`
	UniqueID     string         `json:"unique_id"`
	Device       map[string]any `json:"device"`
}

type MetricRole uint8

const (
	RoleNone MetricRole = iota
	RoleCurrentA
	RoleCurrentB
	RoleCurrentC
	RoleRPM
	RoleCoolantTemp
)

type PreparedRegister struct {
	Config       RegisterConfig
	ModbusType   modbus.RegType
	IsSigned     bool
	IsProcess    bool
	Role         MetricRole
	StateTopic   string
	AnomalyTopic string
	MetricKey    string
	LabelsScore  []prompb.Label
	LabelsYhat   []prompb.Label
	LabelsUpper  []prompb.Label
	LabelsLower  []prompb.Label
}

// ==========================================
// ZERO-ALLOC БУФЕРЫ ПАМЯТИ
// ==========================================

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

// ==========================================
// IN-MEMORY RING BUFFER
// ==========================================

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

// ==========================================
// БУФЕР ДЕТЕКЦИИ ТЕМПЕРАТУРНОГО ДРЕЙФА
// ==========================================

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

	if (maxVal - minVal) < 0.5 {
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

	if math.Abs(slope) < 0.2 || slope < 0 {
		return 0.0
	}

	return slope
}

// ==========================================
// МАТЕМАТИКА QUICKSELECT И MAD
// ==========================================

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

// ==========================================
// РЕЕСТР ДЕТЕКЦИИ АНОМАЛИЙ
// ==========================================

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

// ==========================================
// VICTORIAMETRICS REMOTE WRITE CLIENT
// ==========================================

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

// ==========================================
// ГЛОБАЛЬНОЕ СОСТОЯНИЕ ДЕМОНА
// ==========================================

var (
	models          = make(map[string]ControllerModel)
	userDevices     = []UserDevice{}
	activePollers   = make(map[string]chan struct{})
	mqttSubs        = make(map[string]mqtt.MessageHandler)
	mu              sync.RWMutex
	mqttClient      mqtt.Client
	userDevicesFile string

	vmWriter *VMWriter
)

func main() {
	log.Println("Инициализация gpu_bridge для Home Assistant...")

	controllersPath := os.Getenv("CONTROLLERS_PATH")
	if controllersPath == "" {
		controllersPath = "controllers"
	}

	customControllersPath := os.Getenv("CUSTOM_CONTROLLERS_PATH")
	if customControllersPath == "" {
		customControllersPath = "/config/gpu_modbus_custom/"
	}
	_ = os.MkdirAll(customControllersPath, 0755)

	dataPath := os.Getenv("DATA_PATH")
	if dataPath == "" {
		dataPath = "data"
	}
	_ = os.MkdirAll(dataPath, 0755)
	userDevicesFile = filepath.Join(dataPath, "user_devices.json")

	vmWriteURL := os.Getenv("VM_WRITE_URL")
	if vmWriteURL != "" {
		vmWriter = NewVMWriter(vmWriteURL)
		log.Printf("VictoriaMetrics Remote Write активен: %s", vmWriteURL)
	}

	loadModels(controllersPath, customControllersPath)
	initMQTT()
	loadUserDevices()

	log.Println("Go ГПУ Мост успешно запущен в фоновом режиме Home Assistant")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Остановка фонового процесса ГПУ Моста...")

	mu.Lock()
	for id, stopChan := range activePollers {
		close(stopChan)
		delete(activePollers, id)
	}
	mu.Unlock()

	if vmWriter != nil {
		vmWriter.Close()
	}

	if mqttClient != nil && mqttClient.IsConnected() {
		mqttClient.Disconnect(250)
	}
	log.Println("ГПУ Мост завершил работу.")
}

func loadModels(paths ...string) {
	mu.Lock()
	defer mu.Unlock()

	totalLoaded := 0
	for _, path := range paths {
		files, err := filepath.Glob(filepath.Join(path, "*.json"))
		if err != nil {
			log.Printf("[Models] Ошибка сканирования директории %s: %v", path, err)
			continue
		}

		for _, f := range files {
			data, err := os.ReadFile(f)
			if err != nil {
				continue
			}

			var m ControllerModel
			if err := json.Unmarshal(data, &m); err != nil {
				continue
			}

			models[m.ModelID] = m
			totalLoaded++
			log.Printf("[Models] Загружена модель контроллера: %s [%s] из папки: %s", m.Name, m.ModelID, path)
		}
	}
	if totalLoaded == 0 {
		log.Printf("[Models] Внимание: не найдено файлов моделей в путях: %v", paths)
	}
}

func initMQTT() {
	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		broker = "tcp://127.0.0.1:1883"
	}

	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID(fmt.Sprintf("gpu_bridge_%d", time.Now().UnixNano()))
	opts.SetUsername(os.Getenv("MQTT_USER"))
	opts.SetPassword(os.Getenv("MQTT_PASSWORD"))
	opts.SetConnectTimeout(3 * time.Second)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)

	opts.OnConnect = func(c mqtt.Client) {
		log.Println("[MQTT] Успешно установлено соединение с брокером!")
		mu.RLock()
		for topic, handler := range mqttSubs {
			c.Subscribe(topic, 1, handler)
		}
		mu.RUnlock()
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		log.Printf("[MQTT] Потеряна связь с брокером: %v. Идет автопереподключение...", err)
	}

	mqttClient = mqtt.NewClient(opts)
	token := mqttClient.Connect()
	if token.WaitTimeout(3*time.Second) && token.Error() != nil {
		log.Printf("[MQTT] Брокер пока недоступен: %v. Подключение продолжится в фоне.", token.Error())
	}
}

func loadUserDevices() {
	mu.Lock()
	defer mu.Unlock()

	data, err := os.ReadFile(userDevicesFile)
	if err != nil {
		return
	}

	var devices []UserDevice
	if err := json.Unmarshal(data, &devices); err != nil {
		return
	}
	userDevices = devices

	for _, d := range userDevices {
		dev := d
		log.Printf("Запуск независимого воркера для: %s [%s]", dev.Name, dev.ID)
		go startDevicePolling(dev)
	}
}

func safeMqttPublish(topic string, qos byte, retained bool, payload interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[MQTT] Ошибка публикации: %v", r)
		}
	}()

	if mqttClient == nil || !mqttClient.IsConnected() {
		return
	}
	mqttClient.Publish(topic, qos, retained, payload)
}

func calculateCurrentImbalance(ia, ib, ic float64) float64 {
	if ia < 1.0 && ib < 1.0 && ic < 1.0 {
		return 0.0
	}

	sred := (ia + ib + ic) / 3.0
	if sred == 0 {
		return 0.0
	}

	diffA := math.Abs(ia - sred)
	diffB := math.Abs(ib - sred)
	diffC := math.Abs(ic - sred)

	maxDiff := diffA
	if diffB > maxDiff {
		maxDiff = diffB
	}
	if diffC > maxDiff {
		maxDiff = diffC
	}

	return (maxDiff / sred) * 100.0
}

func initializeSensorsWithZeros(d UserDevice, model ControllerModel) {
	for _, reg := range model.Registers {
		stateTopic := fmt.Sprintf("gpu/%s/%s/state", d.ID, reg.ID)
		safeMqttPublish(stateTopic, 0, false, "0.00")
	}
	imbalanceTopic := fmt.Sprintf("gpu/%s/current_imbalance/state", d.ID)
	safeMqttPublish(imbalanceTopic, 0, false, "0.0")
	safeMqttPublish(fmt.Sprintf("gpu/%s/rpm_jitter/state", d.ID), 0, false, "0.0")
	safeMqttPublish(fmt.Sprintf("gpu/%s/coolant_temp_rate/state", d.ID), 0, false, "0.0")
}

func startDevicePolling(d UserDevice) {
	mu.RLock()
	model, exists := models[d.ModelID]
	mu.RUnlock()

	if !exists {
		log.Printf("Модель контроллера %s не найдена для ГПУ %s", d.ModelID, d.Name)
		return
	}

	if d.TimeoutSec <= 0 {
		d.TimeoutSec = 10
	}

	devAnomalyReg := NewAnomalyRegistry(d.AnomalyWindow, 15, d.AnomalySensitivity)

	for i := 0; i < 5; i++ {
		if mqttClient != nil && mqttClient.IsConnected() {
			break
		}
		time.Sleep(1 * time.Second)
	}

	deviceInfo := map[string]any{
		"identifiers":  []string{d.ID},
		"name":         d.Name,
		"manufacturer": model.Manufacturer,
		"model":        model.Name,
	}

	preparedRegs := make([]PreparedRegister, 0, len(model.Registers))

	for _, reg := range model.Registers {
		discoveryTopic := fmt.Sprintf("homeassistant/sensor/%s/%s/config", d.ID, reg.ID)
		stateTopic := fmt.Sprintf("gpu/%s/%s/state", d.ID, reg.ID)
		anomalyStateTopic := fmt.Sprintf("gpu/%s/%s/anomaly_score", d.ID, reg.ID)
		anomalyDiscoveryTopic := fmt.Sprintf("homeassistant/sensor/%s/%s_anomaly/config", d.ID, reg.ID)

		unit := reg.Unit
		if reg.DeviceClass == "pressure" {
			switch strings.ToLower(reg.Unit) {
			case "kpa":
				unit = "kPa"
			case "bar":
				unit = "bar"
			}
		}

		payload := HADiscoveryPayload{
			Name:              fmt.Sprintf("%s %s", d.Name, reg.Name),
			StateTopic:        stateTopic,
			UnitOfMeasurement: unit,
			DeviceClass:       reg.DeviceClass,
			UniqueID:          fmt.Sprintf("%s_%s", d.ID, reg.ID),
			Device:            deviceInfo,
		}
		jsonData, _ := json.Marshal(payload)
		safeMqttPublish(discoveryTopic, 1, true, jsonData)

		isProc := isProcessMetric(reg)
		if isProc {
			anomalyPayload := HADiscoveryPayload{
				Name:              fmt.Sprintf("%s %s Аномальность", d.Name, reg.Name),
				StateTopic:        anomalyStateTopic,
				UnitOfMeasurement: "score",
				UniqueID:          fmt.Sprintf("%s_%s_anomaly", d.ID, reg.ID),
				Device:            deviceInfo,
			}
			anomalyJson, _ := json.Marshal(anomalyPayload)
			safeMqttPublish(anomalyDiscoveryTopic, 1, true, anomalyJson)
		} else {
			safeMqttPublish(anomalyDiscoveryTopic, 1, true, "")
		}

		regTypeLower := strings.ToLower(reg.Type)
		mbType := modbus.HOLDING_REGISTER
		if regTypeLower == "input" {
			mbType = modbus.INPUT_REGISTER
		}

		prep := PreparedRegister{
			Config:       reg,
			ModbusType:   mbType,
			IsSigned:     isSignedRegister(reg),
			IsProcess:    isProc,
			Role:         detectMetricRole(strings.ToLower(reg.ID)),
			StateTopic:   stateTopic,
			AnomalyTopic: anomalyStateTopic,
			MetricKey:    fmt.Sprintf("%s:%s", d.ID, reg.ID),
		}

		if isProc && vmWriter != nil {
			prep.LabelsScore = buildPromLabels(d.ID, d.Name, reg.ID, "gpu_"+reg.ID+":anomaly_score")
			prep.LabelsYhat = buildPromLabels(d.ID, d.Name, reg.ID, "gpu_"+reg.ID+":yhat")
			prep.LabelsUpper = buildPromLabels(d.ID, d.Name, reg.ID, "gpu_"+reg.ID+":yhat_upper")
			prep.LabelsLower = buildPromLabels(d.ID, d.Name, reg.ID, "gpu_"+reg.ID+":yhat_lower")
		}

		preparedRegs = append(preparedRegs, prep)
	}

	imbalanceStateTopic := fmt.Sprintf("gpu/%s/current_imbalance/state", d.ID)
	imbalanceAnomState := fmt.Sprintf("gpu/%s/current_imbalance/anomaly_score", d.ID)
	imbalanceKey := fmt.Sprintf("%s:current_imbalance", d.ID)

	rpmJitterStateTopic := fmt.Sprintf("gpu/%s/rpm_jitter/state", d.ID)
	rpmJitterAnomState := fmt.Sprintf("gpu/%s/rpm_jitter/anomaly_score", d.ID)
	rpmJitterKey := fmt.Sprintf("%s:rpm_jitter", d.ID)

	tempRateStateTopic := fmt.Sprintf("gpu/%s/coolant_temp_rate/state", d.ID)
	tempRateAnomState := fmt.Sprintf("gpu/%s/coolant_temp_rate/anomaly_score", d.ID)
	tempRateKey := fmt.Sprintf("%s:coolant_temp_rate", d.ID)

	imbalanceJson, _ := json.Marshal(HADiscoveryPayload{
		Name:              fmt.Sprintf("%s Перекос тока", d.Name),
		StateTopic:        imbalanceStateTopic,
		UnitOfMeasurement: "%",
		UniqueID:          fmt.Sprintf("%s_current_imbalance", d.ID),
		Device:            deviceInfo,
	})
	safeMqttPublish(fmt.Sprintf("homeassistant/sensor/%s/current_imbalance/config", d.ID), 1, true, imbalanceJson)

	anomJson, _ := json.Marshal(HADiscoveryPayload{
		Name:              fmt.Sprintf("%s Перекос тока Аномальность", d.Name),
		StateTopic:        imbalanceAnomState,
		UnitOfMeasurement: "score",
		UniqueID:          fmt.Sprintf("%s_current_imbalance_anomaly", d.ID),
		Device:            deviceInfo,
	})
	safeMqttPublish(fmt.Sprintf("homeassistant/sensor/%s/current_imbalance_anomaly/config", d.ID), 1, true, anomJson)

	rjJson, _ := json.Marshal(HADiscoveryPayload{
		Name:              fmt.Sprintf("%s Раскачка оборотов", d.Name),
		StateTopic:        rpmJitterStateTopic,
		UnitOfMeasurement: "rpm",
		UniqueID:          fmt.Sprintf("%s_rpm_jitter", d.ID),
		Device:            deviceInfo,
	})
	safeMqttPublish(fmt.Sprintf("homeassistant/sensor/%s/rpm_jitter/config", d.ID), 1, true, rjJson)

	rjaJson, _ := json.Marshal(HADiscoveryPayload{
		Name:              fmt.Sprintf("%s Раскачка оборотов Аномальность", d.Name),
		StateTopic:        rpmJitterAnomState,
		UnitOfMeasurement: "score",
		UniqueID:          fmt.Sprintf("%s_rpm_jitter_anomaly", d.ID),
		Device:            deviceInfo,
	})
	safeMqttPublish(fmt.Sprintf("homeassistant/sensor/%s/rpm_jitter_anomaly/config", d.ID), 1, true, rjaJson)

	trJson, _ := json.Marshal(HADiscoveryPayload{
		Name:              fmt.Sprintf("%s Скорость нагрева ОЖ", d.Name),
		StateTopic:        tempRateStateTopic,
		UnitOfMeasurement: "°C/h",
		UniqueID:          fmt.Sprintf("%s_coolant_temp_rate", d.ID),
		Device:            deviceInfo,
	})
	safeMqttPublish(fmt.Sprintf("homeassistant/sensor/%s/coolant_temp_rate/config", d.ID), 1, true, trJson)

	traJson, _ := json.Marshal(HADiscoveryPayload{
		Name:              fmt.Sprintf("%s Скорость нагрева ОЖ Аномальность", d.Name),
		StateTopic:        tempRateAnomState,
		UnitOfMeasurement: "score",
		UniqueID:          fmt.Sprintf("%s_coolant_temp_rate_anomaly", d.ID),
		Device:            deviceInfo,
	})
	safeMqttPublish(fmt.Sprintf("homeassistant/sensor/%s/coolant_temp_rate_anomaly/config", d.ID), 1, true, traJson)

	cmdChan := make(chan CommandConfig, 10)

	for _, cmd := range model.Commands {
		discoveryTopic := fmt.Sprintf("homeassistant/button/%s/%s/config", d.ID, cmd.ID)
		commandTopic := fmt.Sprintf("gpu/%s/%s/set", d.ID, cmd.ID)

		btnJson, _ := json.Marshal(HAButtonDiscovery{
			Name:         fmt.Sprintf("%s %s", d.Name, cmd.Name),
			CommandTopic: commandTopic,
			PayloadPress: "PRESS",
			UniqueID:     fmt.Sprintf("%s_%s", d.ID, cmd.ID),
			Device:       deviceInfo,
		})
		safeMqttPublish(discoveryTopic, 1, true, btnJson)

		capturedCmd := cmd
		handler := func(client mqtt.Client, msg mqtt.Message) {
			if string(msg.Payload()) == "PRESS" {
				select {
				case cmdChan <- capturedCmd:
				default:
				}
			}
		}

		mu.Lock()
		mqttSubs[commandTopic] = handler
		mu.Unlock()

		if mqttClient != nil && mqttClient.IsConnected() {
			mqttClient.Subscribe(commandTopic, 1, handler)
		}
	}

	initializeSensorsWithZeros(d, model)

	stopChan := make(chan struct{})

	mu.Lock()
	if oldStop, exists := activePollers[d.ID]; exists {
		close(oldStop)
	}
	activePollers[d.ID] = stopChan
	mu.Unlock()

	rpmBuffer := NewRingBuffer(30)
	tempTrendBuffer := NewTrendBuffer(15 * time.Minute)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[%s] Фоновый воркер остановлен (паника предотвращена): %v", d.Name, r)
			}
		}()

		communicationTimeout := time.Duration(d.TimeoutSec) * time.Second
		lastSuccessfulRead := time.Now()
		linkDown := true

		ticker := time.NewTicker(7 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-stopChan:
				mu.Lock()
				for _, cmd := range model.Commands {
					unsubTopic := fmt.Sprintf("gpu/%s/%s/set", d.ID, cmd.ID)
					delete(mqttSubs, unsubTopic)
					if mqttClient != nil && mqttClient.IsConnected() {
						mqttClient.Unsubscribe(unsubTopic)
					}
				}
				mu.Unlock()
				return

			// Изолированная запись команд
			case cmd := <-cmdChan:
				cmdClient, err := modbus.NewClient(&modbus.ClientConfiguration{
					URL:     "tcp://" + d.Address,
					Timeout: 2 * time.Second,
				})
				if err == nil {
					if err := cmdClient.Open(); err == nil {
						var writeErr error
						if strings.ToLower(cmd.Type) == "coil" {
							writeErr = cmdClient.WriteCoil(cmd.Address, cmd.Value > 0)
						} else {
							writeErr = cmdClient.WriteRegister(cmd.Address, cmd.Value)
						}
						if writeErr != nil {
							log.Printf("[%s] Ошибка выполнения команды '%s': %v", d.Name, cmd.Name, writeErr)
						} else {
							log.Printf("[%s] УСПЕХ: Команда '%s' выполнена (Адрес: %d, Записано: %d)",
								d.Name, cmd.Name, cmd.Address, cmd.Value)
						}
						cmdClient.Close()
					} else {
						log.Printf("[%s] Ошибка подключения для команды '%s': %v", d.Name, cmd.Name, err)
					}
				}

			// Циклический сессионный опрос раз в 7 секунд
			case <-ticker.C:
				if mqttClient == nil || !mqttClient.IsConnected() {
					continue
				}

				client, err := modbus.NewClient(&modbus.ClientConfiguration{
					URL:     "tcp://" + d.Address,
					Timeout: 2 * time.Second,
				})
				if err != nil {
					if !linkDown && time.Since(lastSuccessfulRead) > communicationTimeout {
						linkDown = true
						initializeSensorsWithZeros(d, model)
					}
					continue
				}

				deviceReadError := false
				var currentA, currentB, currentC float64
				var hasCurrentA, hasCurrentB, hasCurrentC bool
				var currentRPM, coolantTemp float64
				var hasRPM, hasCoolantTemp bool
				nowMs := time.Now().UnixMilli()

				if err := client.Open(); err != nil {
					deviceReadError = true
					if linkDown {
						initializeSensorsWithZeros(d, model)
					}
				} else {
					for i := range preparedRegs {
						select {
						case <-stopChan:
							client.Close()
							return
						default:
						}

						prep := &preparedRegs[i]
						reg := prep.Config
						var val float64
						var readErr error

						// Все чтения строго через Holding/Input регистры (без ReadCoil 01H)
						if reg.Bitrate == 32 {
							var raw32 []uint16
							raw32, readErr = client.ReadRegisters(reg.Address, 2, prep.ModbusType)
							if readErr == nil && len(raw32) == 2 {
								combinedInt := int32(uint32(raw32[0])<<16 | uint32(raw32[1]))
								val = float64(combinedInt) * reg.Scale
							} else if readErr == nil {
								readErr = fmt.Errorf("недостаточно регистров")
							}
						} else {
							var raw16 uint16
							raw16, readErr = client.ReadRegister(reg.Address, prep.ModbusType)
							if readErr == nil {
								if raw16 == 32766 || raw16 == 32767 {
									continue
								}
								if prep.IsSigned {
									val = float64(int16(raw16)) * reg.Scale
								} else {
									val = float64(raw16) * reg.Scale
								}
							}
						}

						if readErr != nil {
							deviceReadError = true
							continue
						}

						lastSuccessfulRead = time.Now()
						if linkDown {
							log.Printf("[%s] Связь с ГПУ восстановлена!", d.Name)
							linkDown = false
						}

						safeMqttPublish(prep.StateTopic, 0, false, fmt.Sprintf("%.2f", val))

						if prep.IsProcess {
							anom := devAnomalyReg.IngestAndEvaluate(prep.MetricKey, val)
							if anom.IsReady {
								safeMqttPublish(prep.AnomalyTopic, 0, false, fmt.Sprintf("%.2f", anom.AnomalyScore))

								if vmWriter != nil {
									vmWriter.PushSeries(prep.LabelsScore, anom.AnomalyScore, nowMs)
									vmWriter.PushSeries(prep.LabelsYhat, anom.Median, nowMs)
									vmWriter.PushSeries(prep.LabelsUpper, anom.UpperBound, nowMs)
									vmWriter.PushSeries(prep.LabelsLower, anom.LowerBound, nowMs)
								}
							}
						}

						switch prep.Role {
						case RoleCurrentA:
							currentA = val
							hasCurrentA = true
						case RoleCurrentB:
							currentB = val
							hasCurrentB = true
						case RoleCurrentC:
							currentC = val
							hasCurrentC = true
						case RoleRPM:
							currentRPM = val
							hasRPM = true
						case RoleCoolantTemp:
							coolantTemp = val
							hasCoolantTemp = true
						}
					}

					// Сбрасываем сокет и очищаем буферы шлюза
					client.Close()

					// 1. Расчет перекоса токов
					if hasCurrentA && hasCurrentB && hasCurrentC {
						imbalance := calculateCurrentImbalance(currentA, currentB, currentC)
						safeMqttPublish(imbalanceStateTopic, 0, false, fmt.Sprintf("%.1f", imbalance))

						imbAnom := devAnomalyReg.IngestAndEvaluate(imbalanceKey, imbalance)
						if imbAnom.IsReady {
							safeMqttPublish(imbalanceAnomState, 0, false, fmt.Sprintf("%.2f", imbAnom.AnomalyScore))
						}
					}

					// 2. Расчет раскачки оборотов (RPM Jitter)
					if hasRPM {
						if currentRPM > 100 {
							rpmBuffer.Push(currentRPM)
							minR, maxR, cnt := rpmBuffer.MinMax()
							if cnt >= 10 {
								rpmJitter := maxR - minR
								safeMqttPublish(rpmJitterStateTopic, 0, false, fmt.Sprintf("%.1f", rpmJitter))
								jAnom := devAnomalyReg.IngestAndEvaluate(rpmJitterKey, rpmJitter)
								if jAnom.IsReady {
									safeMqttPublish(rpmJitterAnomState, 0, false, fmt.Sprintf("%.2f", jAnom.AnomalyScore))
								}
							}
						} else {
							rpmBuffer.Reset()
							safeMqttPublish(rpmJitterStateTopic, 0, false, "0.0")
						}
					}

					// 3. Расчет тренда скорости нагрева ОЖ (°C/ч)
					if hasCoolantTemp {
						if coolantTemp > 40 {
							tempTrendBuffer.Push(coolantTemp)
							ratePerHour := tempTrendBuffer.CalculateRatePerHour()
							if ratePerHour < 0 {
								ratePerHour = 0.0
							}
							safeMqttPublish(tempRateStateTopic, 0, false, fmt.Sprintf("%.1f", ratePerHour))

							rAnom := devAnomalyReg.IngestAndEvaluate(tempRateKey, ratePerHour)
							if rAnom.IsReady {
								safeMqttPublish(tempRateAnomState, 0, false, fmt.Sprintf("%.2f", rAnom.AnomalyScore))
							}
						} else {
							tempTrendBuffer.Reset()
							safeMqttPublish(tempRateStateTopic, 0, false, "0.0")
						}
					}
				}

				if deviceReadError && !linkDown {
					if time.Since(lastSuccessfulRead) > communicationTimeout {
						log.Printf("[%s] Авария связи! Нет данных > %v. Сброс в 0.", d.Name, communicationTimeout)
						linkDown = true
						initializeSensorsWithZeros(d, model)
					}
				}
			}
		}
	}()
}
