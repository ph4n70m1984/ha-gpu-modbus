package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	Type    string `json:"type"`  // "register" (Holding Register) или "coil"
	Value   uint16 `json:"value"` // Значение для записи
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
	Bitrate     int     `json:"bitrate"` // 16 или 32 бита
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
// МАТЕМАТИКА QUICKSELECT И MAD (O(N))
// ==========================================

func quickselect(a []float64, k int) float64 {
	left, right := 0, len(a)-1
	for left < right {
		pivot := a[right]
		i := left
		for j := left; j < right; j++ {
			if a[j] <= pivot {
				a[i], a[j] = a[j], a[i]
				i++
			}
		}
		a[i], a[right] = a[right], a[i]
		if i == k {
			return a[k]
		} else if i < k {
			left = i + 1
		} else {
			right = i - 1
		}
	}
	return a[k]
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

	// Адаптивная нижняя граница разброса, предотвращающая деление на нулевой или микроскопический шум
	minMad := math.Abs(median) * 0.02
	if minMad < 0.5 {
		minMad = 0.5
	}
	if mad < minMad {
		mad = minMad
	}

	upperBound := median + r.sensitivity*mad
	lowerBound := math.Max(0, median-r.sensitivity*mad)
	anomalyScore := math.Abs(val-median) / (r.sensitivity * mad)

	// Ограничиваем сверху, чтобы всплески холодного старта не зашкаливали
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

// Фильтр: отсекаем счетчики, монотонно растущие значения и сырые байты
func isProcessMetric(reg RegisterConfig) bool {
	id := strings.ToLower(reg.ID)
	name := strings.ToLower(reg.Name)
	unit := strings.ToLower(reg.Unit)

	if strings.Contains(id, "motochasy") ||
		strings.Contains(id, "energy") ||
		strings.Contains(id, "vyrabotannaya") ||
		strings.Contains(id, "zapusk") ||
		strings.Contains(id, "minuty") ||
		strings.Contains(id, "lsb") ||
		strings.Contains(id, "msb") ||
		strings.Contains(name, "моточасы") ||
		strings.Contains(name, "энергия") ||
		strings.Contains(name, "запуск") ||
		strings.Contains(name, "наработка") ||
		strings.Contains(name, "lsb") ||
		strings.Contains(name, "msb") ||
		unit == "kwh" ||
		unit == "min" ||
		unit == "h" {
		return false
	}
	return true
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

func (w *VMWriter) Push(ts prompb.TimeSeries) {
	if w == nil {
		return
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

	var batch []prompb.TimeSeries

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
		log.Printf("[VMWriter] Ошибка сериализации Protobuf: %v", err)
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
		log.Printf("[VMWriter] Ошибка отправки Remote Write: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		log.Printf("[VMWriter] VictoriaMetrics вернула статус %d: %s", resp.StatusCode, string(b))
	}
}

func makeTimeSeries(labels map[string]string, val float64, tsMs int64) prompb.TimeSeries {
	pbLabels := make([]prompb.Label, 0, len(labels))
	for k, v := range labels {
		pbLabels = append(pbLabels, prompb.Label{Name: k, Value: v})
	}
	return prompb.TimeSeries{
		Labels: pbLabels,
		Samples: []prompb.Sample{
			{Value: val, Timestamp: tsMs},
		},
	}
}

func mergeLabels(base map[string]string, k, v string) map[string]string {
	out := make(map[string]string, len(base)+1)
	for key, val := range base {
		out[key] = val
	}
	out[k] = v
	return out
}

// ==========================================
// ГЛОБАЛЬНОЕ СОСТОЯНИЕ ДЕМОНА
// ==========================================

var (
	models          = make(map[string]ControllerModel)
	userDevices     = []UserDevice{}
	activePollers   = make(map[string]chan struct{})
	mu              sync.RWMutex
	mqttClient      mqtt.Client
	userDevicesFile string

	anomalyReg *AnomalyRegistry
	vmWriter   *VMWriter
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

	anomalyReg = NewAnomalyRegistry(60, 15, 3.0)

	vmWriteURL := os.Getenv("VM_WRITE_URL")
	if vmWriteURL != "" {
		vmWriter = NewVMWriter(vmWriteURL)
		log.Printf("VictoriaMetrics Remote Write активен: %s", vmWriteURL)
	}

	loadModels(controllersPath, customControllersPath)
	initMQTT()
	loadUserDevices()

	log.Println("Go ГПУ Мост успешно запущен в фоновом режиме Home Assistant (HTTP-сервер отключен)")

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
				log.Printf("[Models] Ошибка чтения файла %s: %v", f, err)
				continue
			}

			var m ControllerModel
			if err := json.Unmarshal(data, &m); err != nil {
				log.Printf("[Models] Ошибка парсинга JSON в файле %s: %v", f, err)
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
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		log.Printf("[MQTT] Потеряна связь с брокером: %v. Идет автопереподключение...", err)
	}

	mqttClient = mqtt.NewClient(opts)
	log.Printf("[MQTT] Подключение к брокеру: %s ...", broker)

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
		log.Printf("Файл устройств %s не найден или еще не создан. Ожидание конфигурации из HA.", userDevicesFile)
		return
	}

	var devices []UserDevice
	if err := json.Unmarshal(data, &devices); err != nil {
		log.Printf("Ошибка парсинга %s: %v", userDevicesFile, err)
		return
	}
	userDevices = devices

	// Запуск каждого устройства в полностью независимой горутине
	for _, d := range userDevices {
		dev := d
		log.Printf("Запуск независимого воркера для: %s [%s]", dev.Name, dev.ID)
		go startDevicePolling(dev)
	}
}

func safeMqttPublish(topic string, qos byte, retained bool, payload interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[MQTT] Ошибка публикации в топик %s (паника предотвращена): %v", topic, r)
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

		if isProcessMetric(reg) {
			anomalyTopic := fmt.Sprintf("gpu/%s/%s/anomaly_score", d.ID, reg.ID)
			safeMqttPublish(anomalyTopic, 0, false, "0.00")
		}
	}
	imbalanceTopic := fmt.Sprintf("gpu/%s/current_imbalance/state", d.ID)
	safeMqttPublish(imbalanceTopic, 0, false, "0.0")

	imbalanceAnomalyTopic := fmt.Sprintf("gpu/%s/current_imbalance/anomaly_score", d.ID)
	safeMqttPublish(imbalanceAnomalyTopic, 0, false, "0.00")
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

	for i := 0; i < 5; i++ {
		if mqttClient != nil && mqttClient.IsConnected() {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// 1. MQTT Discovery: базовые регистры и Anomaly Score только для технологических параметров
	for _, reg := range model.Registers {
		discoveryTopic := fmt.Sprintf("homeassistant/sensor/%s/%s/config", d.ID, reg.ID)
		stateTopic := fmt.Sprintf("gpu/%s/%s/state", d.ID, reg.ID)

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
			Device: map[string]any{
				"identifiers":  []string{d.ID},
				"name":         d.Name,
				"manufacturer": model.Manufacturer,
				"model":        model.Name,
			},
		}
		jsonData, _ := json.Marshal(payload)
		safeMqttPublish(discoveryTopic, 1, true, jsonData)

		anomalyDiscoveryTopic := fmt.Sprintf("homeassistant/sensor/%s/%s_anomaly/config", d.ID, reg.ID)

		if isProcessMetric(reg) {
			anomalyStateTopic := fmt.Sprintf("gpu/%s/%s/anomaly_score", d.ID, reg.ID)
			anomalyPayload := HADiscoveryPayload{
				Name:              fmt.Sprintf("%s %s Аномальность", d.Name, reg.Name),
				StateTopic:        anomalyStateTopic,
				UnitOfMeasurement: "score",
				UniqueID:          fmt.Sprintf("%s_%s_anomaly", d.ID, reg.ID),
				Device: map[string]any{
					"identifiers":  []string{d.ID},
					"name":         d.Name,
					"manufacturer": model.Manufacturer,
					"model":        model.Name,
				},
			}
			anomalyJson, _ := json.Marshal(anomalyPayload)
			safeMqttPublish(anomalyDiscoveryTopic, 1, true, anomalyJson)
		} else {
			// Очищаем старые ненужные топики Discovery для счетчиков в брокере
			safeMqttPublish(anomalyDiscoveryTopic, 1, true, "")
		}
	}

	// 2. MQTT Discovery: перекос токов
	imbalanceConfigTopic := fmt.Sprintf("homeassistant/sensor/%s/current_imbalance/config", d.ID)
	imbalanceStateTopic := fmt.Sprintf("gpu/%s/current_imbalance/state", d.ID)

	imbalancePayload := HADiscoveryPayload{
		Name:              fmt.Sprintf("%s Перекос тока", d.Name),
		StateTopic:        imbalanceStateTopic,
		UnitOfMeasurement: "%",
		UniqueID:          fmt.Sprintf("%s_current_imbalance", d.ID),
		Device: map[string]any{
			"identifiers":  []string{d.ID},
			"name":         d.Name,
			"manufacturer": model.Manufacturer,
			"model":        model.Name,
		},
	}
	imbalanceJson, _ := json.Marshal(imbalancePayload)
	safeMqttPublish(imbalanceConfigTopic, 1, true, imbalanceJson)

	imbalanceAnomalyTopic := fmt.Sprintf("homeassistant/sensor/%s/current_imbalance_anomaly/config", d.ID)
	imbalanceAnomalyState := fmt.Sprintf("gpu/%s/current_imbalance/anomaly_score", d.ID)
	imbalanceAnomPayload := HADiscoveryPayload{
		Name:              fmt.Sprintf("%s Перекос тока Аномальность", d.Name),
		StateTopic:        imbalanceAnomalyState,
		UnitOfMeasurement: "score",
		UniqueID:          fmt.Sprintf("%s_current_imbalance_anomaly", d.ID),
		Device: map[string]any{
			"identifiers":  []string{d.ID},
			"name":         d.Name,
			"manufacturer": model.Manufacturer,
			"model":        model.Name,
		},
	}
	anomJson, _ := json.Marshal(imbalanceAnomPayload)
	safeMqttPublish(imbalanceAnomalyTopic, 1, true, anomJson)

	// 3. Команды управления
	cmdChan := make(chan CommandConfig, 5)

	for _, cmd := range model.Commands {
		discoveryTopic := fmt.Sprintf("homeassistant/button/%s/%s/config", d.ID, cmd.ID)
		commandTopic := fmt.Sprintf("gpu/%s/%s/set", d.ID, cmd.ID)

		btnPayload := HAButtonDiscovery{
			Name:         fmt.Sprintf("%s %s", d.Name, cmd.Name),
			CommandTopic: commandTopic,
			PayloadPress: "PRESS",
			UniqueID:     fmt.Sprintf("%s_%s", d.ID, cmd.ID),
			Device: map[string]any{
				"identifiers":  []string{d.ID},
				"name":         d.Name,
				"manufacturer": model.Manufacturer,
				"model":        model.Name,
			},
		}
		btnJson, _ := json.Marshal(btnPayload)
		safeMqttPublish(discoveryTopic, 1, true, btnJson)

		capturedCmd := cmd
		if mqttClient != nil && mqttClient.IsConnected() {
			mqttClient.Subscribe(commandTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
				if string(msg.Payload()) == "PRESS" {
					cmdChan <- capturedCmd
				}
			})
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

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[%s] Фоновый воркер опроса аварийно остановлен (panic): %v", d.Name, r)
			}
		}()

		communicationTimeout := time.Duration(d.TimeoutSec) * time.Second
		lastSuccessfulRead := time.Now()
		linkDown := true

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
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
						log.Printf("[%s] Ошибка подключения Modbus TCP. Сброс показаний.", d.Name)
						linkDown = true
						initializeSensorsWithZeros(d, model)
					}
					continue
				}

				deviceReadError := false
				var currentA, currentB, currentC float64
				var hasCurrentA, hasCurrentB, hasCurrentC bool
				nowMs := time.Now().UnixMilli()

				if err := client.Open(); err != nil {
					deviceReadError = true
					if linkDown {
						initializeSensorsWithZeros(d, model)
					}
				} else {
					for _, reg := range model.Registers {
						var val float64
						var readErr error
						regType := modbus.HOLDING_REGISTER
						if reg.Type == "input" {
							regType = modbus.INPUT_REGISTER
						}

						if reg.Bitrate == 32 {
							var raw32 []uint16
							raw32, readErr = client.ReadRegisters(reg.Address, 2, regType)
							if readErr == nil && len(raw32) == 2 {
								combinedInt := int32(uint32(raw32[0])<<16 | uint32(raw32[1]))
								val = float64(combinedInt) * reg.Scale
							} else if readErr == nil {
								readErr = fmt.Errorf("получено недостаточно регистров")
							}
						} else {
							var raw16 uint16
							raw16, readErr = client.ReadRegister(reg.Address, regType)
							if readErr == nil {
								val = float64(raw16) * reg.Scale
							}
						}

						stateTopic := fmt.Sprintf("gpu/%s/%s/state", d.ID, reg.ID)

						if readErr != nil {
							deviceReadError = true
							if linkDown {
								safeMqttPublish(stateTopic, 0, false, "0.00")
								if isProcessMetric(reg) {
									anomalyTopic := fmt.Sprintf("gpu/%s/%s/anomaly_score", d.ID, reg.ID)
									safeMqttPublish(anomalyTopic, 0, false, "0.00")
								}
							}
							continue
						}

						lastSuccessfulRead = time.Now()
						if linkDown {
							log.Printf("[%s] Связь с контроллером восстановлена", d.Name)
							linkDown = false
						}

						safeMqttPublish(stateTopic, 0, false, fmt.Sprintf("%.2f", val))

						// Оцениваем аномальность только для физических параметров
						if isProcessMetric(reg) {
							anomalyTopic := fmt.Sprintf("gpu/%s/%s/anomaly_score", d.ID, reg.ID)
							metricKey := fmt.Sprintf("%s:%s", d.ID, reg.ID)
							anom := anomalyReg.IngestAndEvaluate(metricKey, val)

							if anom.IsReady {
								safeMqttPublish(anomalyTopic, 0, false, fmt.Sprintf("%.2f", anom.AnomalyScore))

								if anom.AnomalyScore >= 1.0 {
									log.Printf("[%s] АНОМАЛИЯ '%s': значение=%.2f, медиана=%.2f, score=%.2f",
										d.Name, reg.Name, val, anom.Median, anom.AnomalyScore)
								}

								if vmWriter != nil {
									baseLabels := map[string]string{
										"device_id": d.ID,
										"generator": d.Name,
										"register":  reg.ID,
									}
									vmWriter.Push(makeTimeSeries(mergeLabels(baseLabels, "__name__", "gpu_"+reg.ID+":anomaly_score"), anom.AnomalyScore, nowMs))
									vmWriter.Push(makeTimeSeries(mergeLabels(baseLabels, "__name__", "gpu_"+reg.ID+":yhat"), anom.Median, nowMs))
									vmWriter.Push(makeTimeSeries(mergeLabels(baseLabels, "__name__", "gpu_"+reg.ID+":yhat_upper"), anom.UpperBound, nowMs))
									vmWriter.Push(makeTimeSeries(mergeLabels(baseLabels, "__name__", "gpu_"+reg.ID+":yhat_lower"), anom.LowerBound, nowMs))
								}
							}
						}

						switch reg.ID {
						case "current_a":
							currentA = val
							hasCurrentA = true
						case "current_b":
							currentB = val
							hasCurrentB = true
						case "current_c":
							currentC = val
							hasCurrentC = true
						}
					}

					if hasCurrentA && hasCurrentB && hasCurrentC {
						imbalance := calculateCurrentImbalance(currentA, currentB, currentC)
						imbalanceTopic := fmt.Sprintf("gpu/%s/current_imbalance/state", d.ID)
						safeMqttPublish(imbalanceTopic, 0, false, fmt.Sprintf("%.1f", imbalance))

						imbalanceKey := fmt.Sprintf("%s:current_imbalance", d.ID)
						imbAnom := anomalyReg.IngestAndEvaluate(imbalanceKey, imbalance)

						if imbAnom.IsReady {
							imbalanceAnomTopic := fmt.Sprintf("gpu/%s/current_imbalance/anomaly_score", d.ID)
							safeMqttPublish(imbalanceAnomTopic, 0, false, fmt.Sprintf("%.2f", imbAnom.AnomalyScore))
						}

						if imbalance > 15.0 {
							log.Printf("[%s] ВНИМАНИЕ: Высокий перекос токов: %.1f%% (A:%.1fA, B:%.1fA, C:%.1fA)",
								d.Name, imbalance, currentA, currentB, currentC)
						}
					}
					client.Close()
				}

				if deviceReadError && !linkDown {
					if time.Since(lastSuccessfulRead) > communicationTimeout {
						log.Printf("[%s] Авария связи: нет ответа > %v. Сброс в 0.", d.Name, communicationTimeout)
						linkDown = true
						initializeSensorsWithZeros(d, model)
					}
				}

			case cmd := <-cmdChan:
				client, err := modbus.NewClient(&modbus.ClientConfiguration{
					URL:     "tcp://" + d.Address,
					Timeout: 2 * time.Second,
				})

				if err == nil {
					if err := client.Open(); err == nil {
						var writeErr error

						if cmd.Type == "coil" {
							writeErr = client.WriteCoil(cmd.Address, cmd.Value > 0)
						} else {
							writeErr = client.WriteRegister(cmd.Address, cmd.Value)
						}

						if writeErr != nil {
							log.Printf("[%s] Ошибка выполнения команды '%s': %v", d.Name, cmd.Name, writeErr)
						} else {
							log.Printf("[%s] УСПЕХ: Команда '%s' выполнена (Адрес: %d, Записано: %d)", d.Name, cmd.Name, cmd.Address, cmd.Value)
						}
						client.Close()
					} else {
						log.Printf("[%s] Ошибка сокета при выполнении команды '%s': %v", d.Name, cmd.Name, err)
					}
				}

			case <-stopChan:
				for _, cmd := range model.Commands {
					unsubTopic := fmt.Sprintf("gpu/%s/%s/set", d.ID, cmd.ID)
					mqttClient.Unsubscribe(unsubTopic)
				}
				return
			}
		}
	}()
}
