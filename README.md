# ГПУ Modbus Мост для Home Assistant (GPU Modbus-to-MQTT Bridge)

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-orange.svg?style=for-the-badge)](https://github.com/hacs/integration)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

[RU] Полностью бесплатная Open Source интеграция для мониторинга промышленных газопоршневых установок (ГПУ) в Home Assistant. Под капотом работает быстрый бэкенд на **Go (Golang)**, который опрашивает контроллеры по Modbus TCP и мгновенно передает данные в Home Assistant через MQTT Auto-Discovery.

[EN] A free, open-source Home Assistant integration for monitoring industrial gas generator sets (gensets). Powered by a high-performance **Go (Golang)** backend that polls controllers via Modbus TCP and automatically exposes entities to Home Assistant using MQTT Auto-Discovery.

---

## 🚀 Поддерживаемые контроллеры / Supported Controllers

Интеграция поставляется со встроенной базой готовых карт регистров. Вам не нужно знать адреса Modbus — достаточно просто выбрать модель в интерфейсе:
* **DEIF** (например, AGC 150)
* **SmartGen** (все популярные серии)
* **Woodward**
* *Список легко расширяется добавлением JSON-файлов в папку `controllers/`.*

---

## 🛠 Установка / Installation

### 1. Установка через HACS (HACS Installation)
1. Откройте **HACS** в интерфейсе Home Assistant.
2. В правом верхнем углу нажмите на три точки и выберите **Пользовательские репозитории** (Custom repositories).
3. Вставьте ссылку на этот репозиторий: `https://github.com/ph4n70m1984/ha-gpu-modbus`
4. В поле «Категория» выберите **Интеграция** (Integration) и нажмите **Добавить** (Add).
5. Найдите **ГПУ Modbus Мост** в списке HACS и нажмите **Скачать** (Download).
6. **Перезагрузите Home Assistant**.

### 2. Активация (Activation)
1. Перейдите в **Настройки** -> **Устройства и интеграции**.
2. Нажмите **Добавить интеграцию** в правом нижнем углу.
3. Найдите **ГПУ Modbus Мост** и подтвердите установку.
4. Python-обертка автоматически запустит скомпилированный под вашу архитектуру (amd64 или arm64) Go-бинарник в фоне.

---

## ⚙️ Настройка / Configuration

1. После запуска интеграции откройте веб-панель управления в браузере:
   ```text
   http://<IP_АДРЕС_ВАШЕГО_HOME_ASSISTANT>:8080
   
В открывшемся интерфейсе заполните форму:

Название: Удобное имя (например, ГПУ-1)

Адрес: IP и порт контроллера (например, 192.168.1.100:502)

Модель: Выберите контроллер из выпадающего списка.

Таймаут: Укажите время отсутствия данных в секундах. Если связь с контроллером оборвется и таймаут истечет, интеграция автоматически отправит 0.00 по всем сенсорам в Home Assistant, чтобы вы вовремя узнали об аварии.

Нажмите Добавить в Home Assistant.

Устройство мгновенно появится в вашей штатной интеграции MQTT благодаря функции Auto-Discovery со всеми настроенными сенсорами (Мощность, Обороты, Давление масла и т.д.).

💵 Поддержка разработки / Support the Project
[RU] Проект является полностью бесплатным, открытым и поставляется без каких-либо функциональных ограничений. Если этот мост помогает автоматизировать ваше предприятие или экономит ваше время, вы можете поддержать автора:

[EN] This project is completely free, open-source, and comes without any limitations. If this integration helps your business or saves you time, feel free to support the developer:

Boosty: https://boosty.to/your_profile

GitHub Sponsors: https://github.com/sponsors/your_profile

📝 Лицензия / License
Этот проект распространяется под лицензией MIT. Подробнее см. в файле LICENSE.


### 💡 Не забудьте:
Перед тем как сделать `git push`, замените заглушки ссылок `your_profile` на ваши реальные профили на Bo