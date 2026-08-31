# Telegram Analyser - A Kafka and pySpark project

Real-time spike and trend detection pipeline for Telegram messages using Apache Kafka and PySpark.

After running this pipeline, the results will be alerts for when a word is trending across multiple news channles.

There are 2 types of alerts in this pipeline:
1) Trend: these are trends building over time, they can signal a shift in conversation. A good example would be the name "Bibi Netanyahu" trending because of election season.
2) Spike: These are drastic changes in volume in a short amount of time. Our goal is to detect these as soon as possible, as they can signal an event before it even hits the official news. An example would be the name "Yair Netanyahu" spiking, connected to his recent assassination attempt.

The alerts will be printed in the Token Counter worker notebook, in the last cell.
They can easly be directed to any channle.

This is a guide on how to run a demonstration in the Afeka big data VM.

I reccomend skipping the telegram scraping jobs and using the Replay option insted. If you want to run the scrapers go to https://my.telegram.org to generate an api key and put it into config.json 

## Quick run

Just to provide a clear clean run guide for a simulation,skipping  the live and offline scraping, without connecting to Telegram:
1) run setup.sh, than **`source .venv/bin/activate`**
2) run kafka (can use the run_kafka.sh). if not installed run bash run_kafka_install.sh
3) run **`hBaselineCalc.ipynb`** to generate our baselines data table. (although one is already provided)
3) run all of **`wSparkRawToTokens.ipynb`**
4) run all of **`wSparkTokensCounter.ipynb`**, this is where you will see the results
5) run **`pReplay.ipynb`** to simulate the program running over a month of messeges

This is just a small simulation, read the full guide to understand the full pipeline, how to run it live, and how to configure it

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




## Running the Pipeline

### Start Kafka

Make sure your Kafka broker is up and running.
you can use this custom script:

```bash
bash run_kafka.sh

```

### Setting up workers

There are 2 workers in this pipe line, you need to run both:

#### Raw to Tokens:
**`sparkRawToTokens.ipynb`** :
Reads raw messages from Kafka, tokenizes and filters words in parallel using Spark, and publishes valid tokens and thier timeStamp to `telegram-tokens`.

fliters words based on "followed_tokens" list in config

To start worker, just click run all on this notebook, and look at the output in the last cell, should see: "This spark session is live and ready, awating termination"

#### Tokens counter 
Evaluates incoming tokens against baselines for spikes/trends and displays live notifications.

We have 2 implemented versions. one utilising Spark and the other implemented in python.
**You should run only one of these at a time**

* **`wPythonTokensCounter.ipynb`**: Lightweight, low-overhead pure Python consumer.
After developing this version, we relised it might create a bottel neck. that is why we created the Spark version. The reason we kept both is that the over head created by spark might not be worth its utility, and that a Kafka->python script might work better for this part of the pipeline.


* **`wSparkTokensCounter.ipynb`**: PySpark Structured Streaming consumer utilizing micro-batches.

To start worker, just click run all on this notebook (Spark version is reccomended, but they function the same), and look at the output in the last cell, should see: "Stage 2 worker live: Spark/Python version"


**At this point you should have 2 spark notebooks running: a RawToTokens worker, and a TokenCounter worker. one of each.**


### Start Message Producer

Feed messages into the `telegram-raw` queue:

**Live Stream**: Run **`pLive.ipynb`** to listen to Telegram channels and push new incoming messages in real time.


**Replay / Simulation**: Run **`pReplay.ipynb`** to replay historical messages from `messages.db` at accelerated speed.
In these notebook we can simulate the pipeline working on whole months in secends.

for example, you can configurate the SELECT command in this notebook to simulate spesific date you have in your data base, see what trends we caught at what time, and compare to local news or events from the same day
