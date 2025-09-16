
# ETL Exam Project

---

## 📌 Этапы работы

### 1. Подготовка данных и трансфер данных
- Исходный датасет транзакций был загружен в **Yandex Object Storage**.
- В `exe1/code/create_transactions_v2.yql` описан скрипт подготовки данных для YDB.
- С помощью `ydb_connect.bash` и `ydb_import.bash` данные импортировались в YDB.
- Был реализован трансфер данных из YDB в Object Storage с помощью Data Transfer.
    - Были созданы источник и приемник
    - Бакет для приема данных
    - Сам объект трансфера (его конфигурация)

<details>
    <summary><b>Скриншоты</b></summary>
    - ![Кол-во записей в датасете](exe1/screenshots/01_count_table_items.png)
    - ![Эндпоинты](exe1/screenshots/02_endpoints.png)
    - ![Трансфер](exe1/screenshots/03_transfer.png)
    - ![Пример трансформированных данных](exe1/screenshots/04_transfered_data.png)
</details> 

### 2. Airflow + Dataproc
- Создан DAG (`exe2/code/dag.py`), который оркестрирует запуск Spark-задач на Dataproc.
- Логика обработки вынесена в `job.py`.
- Dataproc использовался для подготовки и трансформации данных.

<details>
    <summary><b>Скриншоты</b></summary>
    - ![Логи терминала при выполнении](exe2/screenshots/00_terminal_job_input.png)
    - ![Дата процессинг бакет](exe2/screenshots/01_dataproc_bucket.png)
    - ![Результат](exe2/screenshots/02_result_parquet.png)
</details> 

### 3. Kafka + Spark Streaming
- Реализован **writer** (`kafka_write.py`), который считывает данные из Parquet и отправляет батчи сообщений в Kafka.
- Реализован **reader** (`kafka_read.py`), который:
  - читает поток из Kafka,
  - парсит JSON по схеме,
  - записывает результат в PostgreSQL.
- Данные потоково загружались в кластер **Managed PostgreSQL** в таблицу `transactions`.
- Пример содержимого таблицы:
![Пример данных](exe3/screenshot/data_amount.png)
![Пример данных](exe3/screenshot/data_by_kafka_read.png)

### 4. Визуализация в DataLens
- Настроено подключение PostgreSQL к DataLens (см. [datalens_connect.png](exe4/datalens_connect.png)).
- Построен дашборд с аналитикой транзакций:

![Дашборд](exe4/datalens_dash.png)

**Содержимое дашборда:**
- 📈 Кумулятивная сумма транзакций во времени  
- 🥧 Суммы оплат по типам  
- 📊 Распределение транзакций по месяцам  
- 🥧 Количество транзакций по типам  

---

## 🚀 Использованные технологии
- **Yandex Cloud** (Object Storage, YDB, Dataproc, Managed Kafka, Managed PostgreSQL, DataLens)
- **Apache Spark 3.0.3** (Structured Streaming)
- **Apache Kafka**
- **Airflow**
- **PostgreSQL**
- **Python / PySpark**

---

## 📎 Репозиторий
[github.com/tmanspace/etl_exam](https://github.com/tmanspace/etl_exam)
