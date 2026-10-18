package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

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
