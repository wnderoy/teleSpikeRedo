**Telegram Analiser**

order of files:

telegramScraper: using telethon, scrapes as much messages as it can
outputs->messages.db

inputs<-messages.db
baseline: using spark we tokenise, filter, and calculate each tokens and its avrage apperanse for each hour of the day
outputs->baseline.db

up until now, this is a batch job that can run once a day

---

