package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/simonvetter/modbus"
)

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

	if d.TimeoutSec < 25 {
		d.TimeoutSec = 30
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

					client.Close()

					if hasCurrentA && hasCurrentB && hasCurrentC {
						imbalance := calculateCurrentImbalance(currentA, currentB, currentC)
						safeMqttPublish(imbalanceStateTopic, 0, false, fmt.Sprintf("%.1f", imbalance))

						imbAnom := devAnomalyReg.IngestAndEvaluate(imbalanceKey, imbalance)
						if imbAnom.IsReady {
							safeMqttPublish(imbalanceAnomState, 0, false, fmt.Sprintf("%.2f", imbAnom.AnomalyScore))
						}
					}

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

					if hasCoolantTemp {
						isRunning := (!hasRPM || currentRPM > 1000) && coolantTemp >= 65
						if isRunning {
							tempTrendBuffer.Push(coolantTemp)
							ratePerHour := tempTrendBuffer.CalculateRatePerHour()
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
