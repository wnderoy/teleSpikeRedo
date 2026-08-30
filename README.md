# Telegram Analyser

Real-time spike and trend detection pipeline for Telegram messages using Apache Kafka and PySpark.

This is a guide on how to run a demonstration in the Afeka big data VM.

I reccomend skipping the telegram scraping jobs and using the Replay option insted. If you want to run the scrapers go to https://my.telegram.org to generate an api key and put it into config.json 

## Configuration & Utilities

**`config.json`**: Contains file paths for databases, Kafka server/topic definitions, and Telegram API credentials.

I have removed my personal API keys, you can generate your own at https://my.telegram.org

you will need to follow news channels and well, make sure you follow every channle in the channels list before running the scraper


**`hStatus.ipynb`**: Utility notebook to inspect live status of background workers, check message counts/backlog in Kafka queues, or kill all active pipeline processes.


## Preparations

These are prepertions we can run as batch jobs. we can run them manualy or daily as a cron job.


**`hTelescraper.ipynb`**: Scrapes as much messages as we can from channel list (configured in config). these can be used for baseline calculations or for replays. saved in path configured in config.

I have already scraped the channels and provided the results as `messages.db`.

You can run the scraper your self but it does take setup.

**`hBaselines.ipynb`**: Uses Spark to tokenize, filter, and calculate the average hourly appearance rate for each followed token, saving the baseline matrix into the configured path.

Currently configured to baselines.db. feel free to delete the current baselines.db file and generate it your self.

You can generate baseline rates for any words of your choosing, just change the "followed_tokens" list and run the notebook again

**This notebook featurs SELECT cells to view your current data bases**



---

## Running the Pipeline

### Start Kafka

Make sure your Kafka broker is up and running.
you can use this custom script:

```bash
bash run_kafka.sh

```

### Setting up workers

There are 2 workers in this pipe line:

#### Raw to Tokens:
**`sparkRawToTokens.ipynb`** :
Reads raw messages from Kafka, tokenizes and filters words in parallel using Spark, and publishes valid tokens and thier timeStamp to `telegram-tokens`.

fliters words based on "followed_tokens" list in config


#### Tokens counter
Evaluates incoming tokens against baselines for spikes/trends and displays live notifications.

We have 2 implemented versions. one utilising Spark
* **`wPythonTokensCounter.ipynb`**: Lightweight, low-overhead pure Python consumer.


* **`sparkTokensCounter.ipynb`**: PySpark Structured Streaming consumer utilizing micro-batches.





### 3. Start Message Producer (Choose ONE)

Feed messages into the `telegram-raw` queue:

* **Live Stream**: Run **`producer.ipynb`** to listen to Telegram channels and push new incoming messages in real time.


* **Replay / Simulation**: Run **`replayProducer.ipynb`** to replay historical messages from `messages.db` at accelerated speed.