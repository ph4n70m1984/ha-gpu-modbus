package main

import (
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
	"github.com/simonvetter/modbus"
)

type ControllerModel struct {
	ModelID      string           `json:"model_id"`
	Name         string           `json:"name"`
	Manufacturer string           `json:"manufacturer"`
	Registers    []RegisterConfig `json:"registers"`
}

type RegisterConfig struct {
	Name        string  `json:"name"`
	ID          string  `json:"id"`
	Address     uint16  `json:"address"`
	Type        string  `json:"type"`
	Unit        string  `json:"unit"`
	DeviceClass string  `json:"device_class"`
	Scale       float64 `json:"scale"`
}

type UserDevice struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Address    string `json:"address"`
	ModelID    string `json:"model_id"`
	TimeoutSec int    `json:"timeout_sec"`
}

type HADiscoveryPayload struct {
	Name              string         `json:"name"`
	StateTopic        string         `json:"state_topic"`
	UnitOfMeasurement string         `json:"unit_of_measurement,omitempty"`
	DeviceClass       string         `json:"device_class,omitempty"`
	UniqueID          string         `json:"unique_id"`
	Device            map[string]any `json:"device"`
}

var (
	models          = make(map[string]ControllerModel)
	userDevices     = []UserDevice{}
	activePollers   = make(map[string]chan struct{})
	mu              sync.RWMutex
	mqttClient      mqtt.Client
	userDevicesFile string
	webPath         string
)

func main() {
	controllersPath := os.Getenv("CONTROLLERS_PATH")
	if controllersPath == "" {
		controllersPath = "controllers"
	}
	webPath = os.Getenv("WEB_PATH")
	if webPath == "" {
		webPath = "web"
	}
	dataPath := os.Getenv("DATA_PATH")
	if dataPath == "" {
		dataPath = "data"
	}
	_ = os.MkdirAll(dataPath, 0755)
	userDevicesFile = filepath.Join(dataPath, "user_devices.json")

	loadModels(controllersPath)
	initMQTT()
	loadUserDevices()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.URL.Path == "/" {
			http.ServeFile(w, r, filepath.Join(webPath, "index.html"))
			return
		}
		http.NotFound(w, r)
	})
	mux.HandleFunc("/api/models", handleGetModels)
	mux.HandleFunc("/api/devices", handleDevices)

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Println("Go ГПУ Мост запущен на порту :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка веб-сервера: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Остановка сервиса...")
}

func loadModels(path string) {
	files, _ := filepath.Glob(filepath.Join(path, "*.json"))
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var m ControllerModel
		if err := json.Unmarshal(data, &m); err == nil {
			models[m.ModelID] = m
			log.Printf("Загружена модель: %s", m.Name)
		}
	}
}

func initMQTT() {
	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		broker = "tcp://127.0.0.1:1883"
	}

	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID("gpu_auto_bridge_" + fmt.Sprintf("%d", time.Now().Unix()))
	opts.SetUsername(os.Getenv("MQTT_USER"))
	opts.SetPassword(os.Getenv("MQTT_PASSWORD"))
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)

	mqttClient = mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("MQTT недоступен: %v. Попытки подключения продолжатся в фоне.", token.Error())
	} else {
		log.Println("Успешно подключено к MQTT")
	}
}

func loadUserDevices() {
	mu.Lock()
	defer mu.Unlock()
	data, err := os.ReadFile(userDevicesFile)
	if err != nil {
		return
	}
	json.Unmarshal(data, &userDevices)

	devicesCopy := make([]UserDevice, len(userDevices))
	copy(devicesCopy, userDevices)

	go func() {
		for _, d := range devicesCopy {
			startDevicePolling(d)
		}
	}()
}

func saveUserDevices() {
	data, _ := json.MarshalIndent(userDevices, "", "  ")
	_ = os.WriteFile(userDevicesFile, data, 0644)
}

func handleGetModels(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	list := []ControllerModel{}
	for _, m := range models {
		list = append(list, m)
	}
	json.NewEncoder(w).Encode(list)
}

func handleDevices(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method == http.MethodGet {
		mu.RLock()
		json.NewEncoder(w).Encode(userDevices)
		mu.RUnlock()
		return
	}

	if r.Method == http.MethodPost {
		var d UserDevice
		if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		mu.Lock()
		d.ID = fmt.Sprintf("gpu_%d", time.Now().Unix())
		userDevices = append(userDevices, d)
		saveUserDevices()
		mu.Unlock()

		startDevicePolling(d)
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(d)
		return
	}

	if r.Method == http.MethodPut {
		var updated UserDevice
		if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		mu.Lock()
		found := false
		for i, d := range userDevices {
			if d.ID == updated.ID {
				userDevices[i] = updated
				found = true
				break
			}
		}
		if found {
			saveUserDevices()
		}
		mu.Unlock()

		if found {
			startDevicePolling(updated)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(updated)
		} else {
			http.Error(w, "Устройство не найдено", http.StatusNotFound)
		}
		return
	}

	if r.Method == http.MethodDelete {
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "Параметр id обязателен", http.StatusBadRequest)
			return
		}

		mu.Lock()
		foundIndex := -1
		for i, d := range userDevices {
			if d.ID == id {
				foundIndex = i
				break
			}
		}

		if foundIndex != -1 {
			if stopChan, exists := activePollers[id]; exists {
				close(stopChan)
				delete(activePollers, id)
			}
			userDevices = append(userDevices[:foundIndex], userDevices[foundIndex+1:]...)
			saveUserDevices()
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"message":"Устройство успешно удалено"}`)
		} else {
			http.Error(w, "Устройство не найдено", http.StatusNotFound)
		}
		mu.Unlock()
		return
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
	}
	// Также обнуляем виртуальный сенсор перекоса токов
	imbalanceTopic := fmt.Sprintf("gpu/%s/current_imbalance/state", d.ID)
	safeMqttPublish(imbalanceTopic, 0, false, "0.0")
}

func startDevicePolling(d UserDevice) {
	model, exists := models[d.ModelID]
	if !exists {
		log.Printf("Модель %s не найдена для ГПУ %s", d.ModelID, d.Name)
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

	// Отправка MQTT Discovery для основных регистров
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
	}

	// Отправка MQTT Discovery для виртуальной сущности перекоса токов
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
				log.Printf("[%s] Фоновый поллер опроса остановлен (паника предотвращена): %v", d.Name, r)
			}
		}()

		communicationTimeout := time.Duration(d.TimeoutSec) * time.Second
		lastSuccessfulRead := time.Now()
		linkDown := true

		ticker := time.NewTicker(5 * time.Second)
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
						log.Printf("[%s] Авария связи (ошибка конфигурации)! Сброс в 0.", d.Name)
						linkDown = true
						initializeSensorsWithZeros(d, model)
					}
					continue
				}

				deviceReadError := false

				var currentA, currentB, currentC float64
				var hasCurrentA, hasCurrentB, hasCurrentC bool

				if err := client.Open(); err != nil {
					deviceReadError = true
					if linkDown {
						initializeSensorsWithZeros(d, model)
					}
				} else {
					for _, reg := range model.Registers {
						var raw uint16
						var readErr error

						if reg.Type == "input" {
							raw, readErr = client.ReadRegister(reg.Address, modbus.INPUT_REGISTER)
						} else {
							raw, readErr = client.ReadRegister(reg.Address, modbus.HOLDING_REGISTER)
						}

						stateTopic := fmt.Sprintf("gpu/%s/%s/state", d.ID, reg.ID)

						if readErr != nil {
							deviceReadError = true
							if linkDown {
								safeMqttPublish(stateTopic, 0, false, "0.00")
							}
							continue
						}

						lastSuccessfulRead = time.Now()
						if linkDown {
							log.Printf("[%s] Связь с ГПУ восстановлена!", d.Name)
							linkDown = false
						}

						val := float64(raw) * reg.Scale
						safeMqttPublish(stateTopic, 0, false, fmt.Sprintf("%.2f", val))

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

						if imbalance > 15.0 {
							log.Printf("[%s] ВНИМАНИЕ: Высокий перекос токов: %.1f%% (A:%.1fA, B:%.1fA, C:%.1fA)",
								d.Name, imbalance, currentA, currentB, currentC)
						}
					}

					client.Close()
				}

				if deviceReadError && !linkDown {
					if time.Since(lastSuccessfulRead) > communicationTimeout {
						log.Printf("[%s] Авария связи! Нет данных > %v. Сброс в 0.", d.Name, communicationTimeout)
						linkDown = true
						initializeSensorsWithZeros(d, model)
					}
				}

			case <-stopChan:
				return
			}
		}
	}()
}
