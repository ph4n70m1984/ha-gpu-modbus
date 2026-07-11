package main

import (
	"encoding/json"
	"fmt"
	"log"
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
	w.Header().Set("Content-Type", "application/json")

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

// Заполнение топиков нулями для инициализации состояния в Home Assistant
func initializeSensorsWithZeros(d UserDevice, model ControllerModel) {
	// СТРОГАЯ ЗАЩИТА: Проверяем, что клиент вообще существует и подключен
	if mqttClient == nil || !mqttClient.IsConnected() {
		log.Printf("[%s] Предупреждение: Попытка отправить нули до инициализации MQTT", d.Name)
		return
	}

	for _, reg := range model.Registers {
		stateTopic := fmt.Sprintf("gpu/%s/%s/state", d.ID, reg.ID)
		mqttClient.Publish(stateTopic, 0, false, "0.00")
	}
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

	// Отправка MQTT Discovery
	for _, reg := range model.Registers {
		if mqttClient == nil {
			continue
		}

		discoveryTopic := fmt.Sprintf("homeassistant/sensor/%s/%s/config", d.ID, reg.ID)
		stateTopic := fmt.Sprintf("gpu/%s/%s/state", d.ID, reg.ID)

		unit := reg.Unit
		if reg.DeviceClass == "pressure" {
			unit = strings.ToLower(reg.Unit)
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
		mqttClient.Publish(discoveryTopic, 1, true, jsonData).Wait()
	}

	// Инициализируем сущности нулями только если MQTT подключен
	if mqttClient != nil && mqttClient.IsConnected() {
		initializeSensorsWithZeros(d, model)
	}

	stopChan := make(chan struct{})

	mu.Lock()
	if oldStop, exists := activePollers[d.ID]; exists {
		close(oldStop)
	}
	activePollers[d.ID] = stopChan
	mu.Unlock()

	go func() {
		// Безопасный отлов паник
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[%s] Внутренняя ошибка (паника) предотвращена: %v", d.Name, r)
			}
		}()

		client, err := modbus.NewClient(&modbus.ClientConfiguration{
			URL:     "tcp://" + d.Address,
			Timeout: 2 * time.Second,
		})
		if err != nil {
			log.Printf("[%s] Ошибка создания клиента Modbus: %v", d.Name, err)
			return
		}

		communicationTimeout := time.Duration(d.TimeoutSec) * time.Second
		lastSuccessfulRead := time.Now()

		// Начинаем со статуса true, чтобы первый же пропуск тикера корректно обработал отсутствие связи
		linkDown := true

		if err := client.Open(); err == nil {
			linkDown = false
			log.Printf("[%s] Успешное первое подключение к Modbus TCP.", d.Name)
		} else {
			log.Printf("[%s] Контроллер недоступен при старте. Будет отправлен повторный пул нулей.", d.Name)
			// Убрали отсюда прямой вызов initializeSensorsWithZeros, так как он выполнится на первом тике
		}

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		defer client.Close()

		for {
			select {
			case <-ticker.C:
				// Если MQTT упал или реконнектится, мягко пропускаем итерацию без паники
				if mqttClient == nil || !mqttClient.IsConnected() {
					continue
				}

				_ = client.Open()
				deviceReadError := false

				for _, reg := range model.Registers {
					var raw uint16
					var err error
					if reg.Type == "input" {
						raw, err = client.ReadRegister(reg.Address, modbus.INPUT_REGISTER)
					} else {
						raw, err = client.ReadRegister(reg.Address, modbus.HOLDING_REGISTER)
					}

					stateTopic := fmt.Sprintf("gpu/%s/%s/state", d.ID, reg.ID)

					if err != nil {
						deviceReadError = true
						if linkDown {
							mqttClient.Publish(stateTopic, 0, false, "0.00")
						}
						continue
					}

					lastSuccessfulRead = time.Now()
					if linkDown {
						log.Printf("[%s] Связь с ГПУ восстановлена!", d.Name)
						linkDown = false
					}

					val := float64(raw) * reg.Scale
					mqttClient.Publish(stateTopic, 0, false, fmt.Sprintf("%.2f", val))
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
