# 🔌 ГПУ Modbus Мост для Home Assistant

### GPU Modbus-to-MQTT Bridge

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8.svg?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
[![Home Assistant](https://img.shields.io/badge/Home%20Assistant-Integration-41BDF5.svg?style=for-the-badge&logo=home-assistant&logoColor=white)](https://www.home-assistant.io/)
[![VictoriaMetrics](https://img.shields.io/badge/VictoriaMetrics-Remote%20Write-621773.svg?style=for-the-badge)](https://victoriametrics.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-amd64%20%7C%20arm64-blueviolet.svg?style=for-the-badge)]()

> Высокопроизводительный промышленный мост на **Go (Golang)** для непрерывного опроса контроллеров газопоршневых (**ГПУ**) и дизельных (**ДГУ**) электростанций по протоколу **Modbus TCP** с автоматической публикацией в **Home Assistant** через **MQTT Discovery** и экспортом метрик в **VictoriaMetrics Remote Write**.

Интеграция снабжена математическим модулем детекции аномалий в реальном времени (**MAD / Quickselect**) и предиктивной аналитикой тепловых и динамических предвестников аварий.

---

## ⚡ Ключевые возможности архитектуры

| Возможность | Описание |
|---|---|
| 🧵 **Параллельный опрос установок** | Каждая ГПУ опрашивается в изолированной горутине Go. Отключение, авария или задержка ответа ГПУ-1 никак не влияют на опрос ГПУ-2. |
| 🔗 **Постоянное TCP-соединение** | Устранено открытие и закрытие сокетов каждую секунду. Контроллеры (DEIF, ComAp, SmartGen) больше не перегружаются сессиями `TIME_WAIT`. |
| 🛡️ **Неразрушающий опрос** | Единичный пропуск Modbus-пакета или задержка ответа отдельного регистра не приводят к обрыву сессии и не сбрасывают показания в `0.00`. |
| 📈 **Детекция аномалий (MAD)** | $O(N)$ Quickselect — оценка отклонений без тяжёлых сортировок и без аллокаций памяти (`sync.Pool`). |
| 🔢 **Фильтрация счётчиков** | Накопительные счётчики (моточасы, кВт·ч, пуски, LSB/MSB) автоматически исключаются из детектора аномалий. |
| 📤 **VictoriaMetrics Remote Write** | Встроенный клиент **Protobuf + Snappy** с буферизацией и асинхронной пакетной отправкой временных рядов. |

---

## 🔬 Предиктивная аналитика — Раннее обнаружение аварий

В мост встроены **два специализированных диагностических детектора**, выявляющих нештатную работу **за 1–2 часа до защитного останова** контроллера.

### 1️⃣ Раскачка оборотов (`rpm_jitter`) — единица: **rpm**

> **Физика процесса:** При перегреве, детонации или обеднении смеси двигатель начинает терять крутящий момент. Электронный регулятор скорости (Governor) начинает совершать паразитные колебания («пилить» частоту) с амплитудой $\pm 5\text{–}10\text{ rpm}$ в попытках удержать 50 Hz.

**Формула** — скользящий размах колебаний пик-пик в окне 30 секунд:

$$
\text{RPM Jitter} = \max_{30s}(\text{RPM}) - \min_{30s}(\text{RPM})
$$

**Уставки:**

| Значение | Статус |
|---|---|
| До **3.0 rpm** | ✅ Идеальная стабильность |
| **5.0 – 8.0 rpm** | ⚠️ Предупреждение, неустойчивость регулятора |
| **> 10.0 rpm** | 🚨 Критическая раскачка, риск аварийного срыва генератора (Underfrequency Trip) |

---

### 2️⃣ Скорость нагрева ОЖ (`coolant_temp_rate`) — единица: **°C/h**

> **Физика процесса:** Медленный перегрев двигателя (забитый радиатор, отказ вентилятора, залипший термостат) развивается в течение 2–3 часов. Из-за плавного роста стандартные секундные пороги не реагируют до момента достижения аварийных 95 °C.

**Формула** — оценка наклона тренда методом наименьших квадратов (МНК) в скользящем окне 15 минут:

$$
\text{Coolant Rate} = \frac{n \sum (t \cdot T) - (\sum t)(\sum T)}{n \sum t^2 - (\sum t)^2} \quad [^\circ\text{C}/\text{ч}]
$$

**Уставки:**

| Значение | Статус |
|---|---|
| До **2.0 °C/ч** | ✅ Тепловой баланс в норме |
| **> 4.0 °C/ч** при стабильной нагрузке | ⚠️ Тревога, деградация контура охлаждения |
| **> 8.0 °C/ч** при T > 85 °C | 🚨 Неконтролируемый терморазгон (Thermal Runaway) |

---

## 🎛 Модели контроллеров из коробки

Карты регистров хранятся в формате JSON в директории `controllers/` (пользовательские — в `/config/gpu_modbus_custom/`):

| Производитель | Серии |
|---|---|
| 🔵 **DEIF** | AGC 150, AGC 200, CGC |
| 🟢 **ComAp** | InteliLite, InteliCompact, InteliGen |
| 🔴 **SmartGen** | HGM, HMC |
| 🟡 **Woodward** | Easygen |

---

## 📦 Сборка и развертывание

> ⚠️ Для избежания сетевых таймаутов HACS при скачивании бинарных файлов рекомендуется **прямая сборка** на целевой системе.

```bash
# 1. Завершите старые процессы демона
killall -9 gpu_bridge_amd64 gpu_bridge_arm64 2>/dev/null || pkill -9 -f gpu_bridge

# 2. Очистите каталог от предыдущих бинарников
rm -f /config/custom_components/gpu_modbus/bin/gpu_bridge_*
cd /config/custom_components/gpu_modbus

# 3. Компиляция (выберите архитектуру):

# Для x86_64 (Intel, AMD, Proxmox VM):
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/gpu_bridge_amd64 main.go

# Для ARM64 (Raspberry Pi 4/5, Home Assistant Green/Yellow):
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/gpu_bridge_arm64 main.go

# 4. Назначьте права на исполнение
chmod +x bin/gpu_bridge_*
```

---

## 📋 Рекомендуемые параметры конфигурации

Параметры устройств настраиваются в файле конфигурации `data/user_devices.json` (или через UI Home Assistant):

```json
[
  {
    "id": "gpu_1",
    "name": "ГПУ-1",
    "address": "192.168.1.101:502",
    "model_id": "deif_agc150",
    "timeout_sec": 15,
    "anomaly_window": 60,
    "anomaly_sensitivity": 3.0
  },
  {
    "id": "gpu_2",
    "name": "ГПУ-2",
    "address": "192.168.1.102:502",
    "model_id": "deif_agc150",
    "timeout_sec": 15,
    "anomaly_window": 60,
    "anomaly_sensitivity": 3.0
  }
]
```

| Параметр | Рекомендуемое значение | Описание |
|---|---|---|
| `address` | `IP:502` | Сетевой адрес и порт контроллера Modbus TCP |
| `timeout_sec` | `15` | Таймаут отсутствия связи. Значение менее **10 секунд** не рекомендуется во избежание ложных срабатываний при технологических задержках RS-485 шлюзов. |
| `anomaly_window` | `60` | Размер скользящего буфера в секундах для расчёта MAD-медианы |
| `anomaly_sensitivity` | `3.0` | Множитель порога аномальности ($S \times \text{MAD}$). Значение `3.0` соответствует отклонению $3\sigma$ нормального распределения (**правило трёх сигм**). |

---

## 🌐 Переменные окружения демона

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `MQTT_BROKER` | `tcp://127.0.0.1:1883` | Адрес MQTT-брокера (Mosquitto) |
| `MQTT_USER` | *(пусто)* | Имя пользователя MQTT |
| `MQTT_PASSWORD` | *(пусто)* | Пароль пользователя MQTT |
| `VM_WRITE_URL` | *(пусто)* | URL эндпоинта VictoriaMetrics Remote Write (например: `http://victoriametrics:8428/api/v1/write`) |
| `DATA_PATH` | `data` | Путь к каталогу с файлом устройств `user_devices.json` |
| `CONTROLLERS_PATH` | `controllers` | Каталог со встроенными JSON-моделями контроллеров |
| `CUSTOM_CONTROLLERS_PATH` | `/config/gpu_modbus_custom/` | Каталог для пользовательских карт регистров |

---

## 🔔 Пример автоматизации предупреждений в Home Assistant

```yaml
alias: "ГПУ: Оповещение о предвестниках аварии"
description: "Оповещение в Telegram при фиксации раскачки регулятора или перегрева"
trigger:
  - platform: numeric_state
    entity_id: sensor.gpu_2_raskachka_oborotov
    above: 8.0
    for:
      seconds: 15
    id: rpm_hunting

  - platform: numeric_state
    entity_id: sensor.gpu_2_skorost_nagreva_ozh
    above: 6.0
    for:
      minutes: 5
    id: thermal_drift

condition:
  - condition: numeric_state
    entity_id: sensor.gpu_2_aktivnaia_moshchnost_summarnaia
    above: 20.0

action:
  - choose:
      - conditions:
          - condition: trigger
            id: rpm_hunting
        sequence:
          - action: notify.telegram
            data:
              message: >
                ⚠️ Внимание! На ГПУ-2 зафиксирована раскачка оборотов: 
                {{ states('sensor.gpu_2_raskachka_oborotov') }} rpm!
                Возможна потеря устойчивости регулятора или обеднение смеси.

      - conditions:
          - condition: trigger
            id: thermal_drift
        sequence:
          - action: notify.telegram
            data:
              message: >
                🔥 Внимание! На ГПУ-2 зафиксирован неконтролируемый рост температуры: 
                +{{ states('sensor.gpu_2_skorost_nagreva_ozh') }} °C/ч!
                Текущая температура: {{ states('sensor.gpu_2_temperatura_okhlazhdayushchey_zhidkosti') }} °C.

mode: single
```

---

## 📝 Лицензия

Этот проект распространяется под лицензией **MIT**.
Подробнее см. в файле [LICENSE](LICENSE).

---

<p align="center">
  <sub>Сделано с ⚡ для промышленной автоматизации на Home Assistant</sub>
</p>