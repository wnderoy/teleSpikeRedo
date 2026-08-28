**Telegram Analiser**

order of files:

**telegramScraper:** using telethon, scrapes as much messages as it can
outputs->messages.db

inputs<-messages.db
**baseline:** using spark we tokenise, filter, and calculate each tokens and its avrage apperanse for each hour of the day
outputs->baseline.db

up until now, this is a batch job that can run once a day



# Pipeline Execution Guide

---

## 1. One-Time Setup (Offline Data Prep)

Run these once to prepare the local datasets:

* **`telegramScraper.ipynb`**
Scrapes historical Telegram messages into `messages.db`.
* **`baseline.ipynb`**
Computes hourly token baselines from `messages.db` and writes them to `baselines.db`.

---

## 2. Infrastructure Setup

Start the messaging broker before running the streaming pipeline:

* **Terminal:**
```bash
bash run_kafka.sh

```



---

## 3. Streaming Engine (Start Listeners)

Run all cells in these notebooks from top to bottom (both cells stay active with `[*]`):

1. **`sparkRawToTokens.ipynb`** (Stage 1)
Creates Kafka topics, tokenizes incoming messages, and pushes tokens to `telegram-tokens`.
2. **`sparkTokensCounter.ipynb`** (Stage 2)
Loads baselines, connects to `telegram-tokens`, and monitors leaky-bucket spike levels.

---

## 4. Message Source (Choose ONE)

Pick how you want to feed messages into the running pipeline:

* **Option A: Historical Replay**
Run **`replayProducer.ipynb`** (Cell 2) to replay messages from `messages.db`.
* **Option B: Live Stream**
Run **`producer.ipynb`** (Cell 3) to stream live incoming Telegram messages in real time.

---

## Utilities

* **`00_healthcheck.ipynb`**
Inspects running components, database states, and pending Kafka queue backlog.
* **Cleanup Cell:**
Kills active workers, clears checkpoints, and purges Kafka topics for a clean restart.

