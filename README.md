# Weather ETL — Open-Meteo → PostgreSQL + Parquet

Инкрементный ETL-пайплайн для сбора почасовых погодных данных из Open-Meteo Historical API с сохранением в PostgreSQL. Каждый запуск загружает только новые записи относительно последнего успешного запуска (паттерн high-water mark). Дополнительно каждый запуск сохраняет инкремент в локальную файловую систему в колоночном сжатом формате Parquet.

---

## Источник данных

**Open-Meteo Historical Weather API** — бесплатный API без регистрации и токена.

Используемый эндпоинт: `https://archive-api.open-meteo.com/v1/archive`

### Города

| Город | Широта | Долгота |
|---|---|---|
| Moscow | 55.7558 | 37.6173 |
| Saint Petersburg | 59.9343 | 30.3351 |
| Novosibirsk | 55.0084 | 82.9357 |

### Собираемые переменные (почасовые)

| Переменная | Описание | Единица |
|---|---|---|
| `temperature_2m` | Температура воздуха на высоте 2 м | °C |
| `precipitation` | Осадки (дождь + снег) | мм |
| `windspeed_10m` | Скорость ветра на высоте 10 м | км/ч |
| `cloudcover` | Облачность | % |

---

## Схема базы данных

```sql
CREATE TABLE weather.hourly_data (
    id              BIGSERIAL PRIMARY KEY,
    city            TEXT          NOT NULL,
    datetime        TIMESTAMPTZ   NOT NULL,
    temperature_2m  FLOAT,
    precipitation   FLOAT,
    windspeed_10m   FLOAT,
    cloudcover      INT,
    loaded_at       TIMESTAMPTZ,
    etl_id          TEXT,
    UNIQUE (city, datetime)
);
```

Уникальный ключ `(city, datetime)` защищает таблицу от дублей при повторных запусках. Поле `etl_id` содержит UUID запуска — он одинаков для всех строк одного запуска и присутствует в каждой строке лога, что позволяет точно отследить происхождение любой записи.

---

## Структура проекта
weather_etl/

├── config.yaml # Конфигурация ETL (города, переменные, схема БД, параметры ретраев)

├── etl.ipynb # Основной ETL-ноутбук

├── requirements.txt # Зависимости Python

├── .gitignore

└── dags/

└── weather_dag.py # Airflow DAG с ежедневным расписанием


---

## Архитектура ETL

read_hwm() — читает MAX(datetime) по каждому городу из БД


fetch_weather() — запрашивает Open-Meteo за период (hwm + 1 день → вчера)
ретраи до 3 раз при сетевых ошибках (tenacity)

parse_rows() — преобразует массивы API-ответа в список строк


anti-join — отсекает уже загруженные (city, datetime) пары


persist_dataframe() — INSERT в PostgreSQL (ON CONFLICT DO NOTHING)
— запись Parquet-файла с zstd-сжатием


При каждом запуске генерируется уникальный `etl_id` (UUID4). Он записывается:
- в каждую строку лога: `etl_id=<uuid> | сообщение`
- в каждую строку, загруженную в БД
- в Parquet-файл текущего инкремента

---

## Быстрый старт

### Требования

- Python 3.9+
- PostgreSQL с базой данных `weather`

### 1. Клонировать репозиторий

```bash
git clone https://github.com/kdmitrykk/ETK_meteo.git
cd weather-etl
```

### 2. Установить зависимости

```bash
pip install -r requirements.txt
```

### 3. Настроить переменные окружения

```bash
cp .env.example .env
```

Заполнить `.env`:

DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather
DB_USER=postgres
DB_PASSWORD=ваш_пароль


### 4. Создать схему в базе данных

```sql
CREATE SCHEMA IF NOT EXISTS weather;
```

### 5. Запустить ETL

Открыть `etl.ipynb` в Jupyter и выполнить все ячейки. При первом запуске данные загружаются начиная с `start_date_fallback` из `config.yaml` до вчерашнего дня. При последующих запусках загружается только новый день.

---

## Выходные данные

### База данных

Строки добавляются в таблицу `weather.hourly_data`. Дубликаты по `(city, datetime)` игнорируются автоматически.

### Parquet-файлы

Каждый запуск создаёт файл в папке `data/`:

data/weather_increment_YYYYMMDD_HHmmss.parquet


Формат Parquet — колоночный, что эффективно для аналитических запросов. Сжатие `zstd` обеспечивает хороший баланс между скоростью и размером файла.

---

## Airflow DAG

Файл `dags/weather_dag.py` описывает DAG с ежедневным расписанием (`0 6 * * *` UTC):

check_db_connection >> run_weather_etl


Для запуска скопировать папку `dags/` в директорию Airflow. DAG появится в веб-интерфейсе автоматически.

---

## Справочник по конфигурации

Все не-секретные настройки хранятся в `config.yaml`:

| Ключ | Описание |
|---|---|
| `database.schema` / `table` | Схема и таблица назначения |
| `cities` | Список городов с координатами |
| `fetch.hourly_variables` | Список запрашиваемых переменных |
| `fetch.start_date_fallback` | Начальная дата для первого запуска |
| `fetch.timezone` | Часовой пояс для временных меток Open-Meteo |
| `http.timeout` | Таймаут HTTP-запроса в секундах |
| `retry.attempts` / `wait_seconds` | Параметры ретраев (tenacity) |
| `logging.level` / `file_name` | Уровень логирования и путь к файлу |
| `filesystem.output_dir` | Директория для Parquet-файлов |
| `filesystem.compression` | Кодек сжатия Parquet (`zstd`, `snappy`, `gzip`) |

---

## Зависимости

| Пакет | Назначение |
|---|---|
| `requests` | HTTP-запросы к Open-Meteo API |
| `tenacity` | Ретраи при сетевых ошибках |
| `polars` | Обработка данных и запись Parquet |
| `sqlalchemy` | Абстракция подключения к БД |
| `psycopg2-binary` | Драйвер PostgreSQL |
| `pendulum` | Работа с датами и часовыми поясами |
| `pyyaml` | Чтение конфигурации из YAML |
| `python-dotenv` | Загрузка секретов из `.env` |
| `apache-airflow` | Оркестрация DAG (устанавливается отдельно) |
