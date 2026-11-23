# BlueSky Steaming Pipeline Project - DS3022

**Author:** Victoria Martinez
**DS 3022 Data Project 3, Fall 2026**

> There is no interview for this project: this README is where you explain and defend your pipeline. Fill in the title, your name and every italic placeholder, then delete this note. Keep the headings: `kit/check_project.py` looks for them.

## Data source
This project uses Bluesky Jetstream, a push-based source accessed through a WebSocket connection. The producer sends each received event to Kafka for downstream processing. Jetstream does not require a subscription message after connecting `SUBSCRIBE = NONE`.

Research question: How does the volume of Bluesky posts vary over time by language?

Originally, I used Coinbase to run this project, however, I encounted many issues with my certificate/security settings. I had to re-start the project from scratch twice but once I switched to BlueSky, I was able to work through the project. 

## How to run
### 1. Prerequisties and installing dependencies: 
Needed: 
- Python 3.13 
- Docker Desktop (open and have running before )

Clone this repository and then open a terminal in its root folder
(`ds3022-streaming`, and ensure that it contains the `docker-compose.yml` file).

Needed commands: 
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install requests tornado quixstreams duckdb prefect matplotlib certifi
```
### 2. Running on the committed sample: 
- Open up a terminal and set up a virtual environment: `source /Users/.../ds3022-streaming/.venv/bin/activate`
- Navigate to the project3 folders and test the sample

```bash
cd project3
python flow.py data/sample
python flow.py data/sample
```
- Ensure that both runs report the same 4 numbers. 
- Because the sample data uses a seperate database so that it doesn't over 



```bash
cd project3
python flow.py data/sample
python flow.py data/sample
```




```bash
cd project3
python flow.py data/sample      # runs without Kafka, on the committed sample
```

## Architecture

_(replace: a diagram (an image in docs/ or a text diagram) of producer → topic → lander → raw Parquet → flow tasks → tables → charts, with the names of your files, topic, tables and charts.)_



## Design decisions

Answer each in 2-4 sentences, in your own words, with your own numbers.

1. **Key and partitions.** _(replace: which key, why, and what it guarantees about order. What would go wrong with another key?)_
2. **Batch size and time.** _(replace: your batch size and time limit. With your source's rate, which rule closes most batches? How many files per hour does that make?)_
3. **Offsets after the file.** _(replace: what happens if the lander crashes after writing a file but before storing offsets, and why you accept that.)_
4. **Duplicates.** _(replace: what makes two records the same Kafka record, and what makes them the same thing. Which SQL removes each kind?)_
5. **Windows and time.** _(replace: which time column your analysis uses (event time or landing time), which window size, and why.)_
6. **Ten times the volume.** _(replace: what in your pipeline would break or slow down first at 10x your rate, and what you would change.)_

## Chaos round

| Card | Prediction | What I did | Numbers before → after | Explanation |
|---|---|---|---|---|
| D1 Crash between file and offsets | I predict that the lander will try to read the crashed batch again because the offsets were not stored properly. | Kept the producer stopped and used the existing backlog. Ran `"$TEST_PY" -u lander.py crash`, then ran `"$TEST_PY" -u lander.py once`, making sure to count between each run. The restart wrote one file before hitting a Python scope error. | Raw 54,612 → 56,612; malformed 0 → 0; unique Kafka records 54,612 → 55,780; unique posts 53,956 → 55,122. | The intentional crash left a Parquet file on disk without storing its offsets. The restart wrote 832 replayed records and 168 not previously landed. The replayed copies increased the raw count but not the unique Kafka-record count. |
| D2 Poison message | I predict that the non-JSON message with be recorded with both a raw row and a malformed row but not stop the lander.| I first created the test topic in T4 and then in T2, started the test lander. After following the instructions to send one malformed message in T4, I verified the result in T2 and T3. Between this and the previous test, I had something weird happen with my lander/producer and somehow acquired over a million messages hence the number difference (I spoke to Prof. Pons about this in class and had to raise the soft limit in the terminal to counter the huge increse in the numbers).| Numbers changed from: raw 1,462,631 → 1,462,632 ,  malformed 0 → 1, unique Kafka records and unique did+rkey stayed the same. 
 | TODO |
| TODO (D3, D4 or D5) | D3: I predict that the broker 1 will go down and as leader. However, high-watermarks will not increase | TODO | My prediction that high-watermakrs would not increase was incorrect, as when I tested two different times while broker 1 was still down, the high-watermakers number increased showing that the topic is still accepting records during the outage. Numbers changed from 1,462,632 → 1,487,245, unique Kafka records 1,461,631 → 1,486,244, unique did+rkey 1,433,497 →  1,457,870| TODO |
| Chaos Round | Raw | Malformed| Unique Kafka Records | Unique did+rkey |
|---|---|---|---|---|
D1 a | 55,612 | 0 | 55,612 | 54,954
D1 b | 56,612 | 0 | 55,780 | 55,122
D2 a | 1,462,631 | 0 | 1,461,631 | 1,433,497
D2 b | 1,462,632 | 1 | 1,461,631 | 1,433,497
D3 a | 1,462,632 | 1 | 1,461,631 | 1,433,497
D3 b | 1,487,245 | 1 | 1,486,244 | 1,457,870

## The four numbers

From the flow log on the full data (screenshot in `docs/`):

| Number | Full data | Sample (`data/sample`) |
|---|---:|---:|
| Raw records landed | 1487245 | 5000 |
| Malformed (kept in raw) | 1 | 0|
| Unique Kafka records | 1486244 | 5000 |
| Unique posts | 1457870 | 5000 |



## Challenges / Obstacles
The two largest challenges I faced with this project was a combination of security measures and lagging times to run any script. First, I encountered a certificate issues which presented itself multiple times in class. Each time, I would seek to reset my certificate and reopen the project. After 2 sessions of this, I decided to restart the entire project and use another data source. This solved many of the security issues I encountered but added a lagging issue. Every single time I sought to run a script (lander, producer, flow, etc.) it would take over 45 mins to run meaning that any edits would take hours to figure out. Because of this, I consistently encountered time-out issues or long delays from either a failed certificate (which would only appear after 30+ mins of running) or I would have to wait an hour for lander.py or another script to receive any files. 

To solve these two issues, I first updated all of my security certificates to access the databases. After that, I changed from Coinbase to BlueSky to make the connection/data transfer time even faster. And I stopped using VS code terminals to run any processes as running straight from my terminal was comparatively faster, however, I didn't discover that until Friday so unfortunately, I spent an entire week working at a quarter of the pace I could have due to the delays from the runtime. 

I also spoke to Professor Pons about both of these issues and medicial appointments/issues that popped up while I was working on this project, making it much harder to properly attend class and complete the assignment in a more timely manner. 

## Analysis

_(replace: one clear paragraph. Your research question, what the data shows, one finding that surprised you or is not obvious, and one limitation of your data or method.)_

## Plot / Visualization

_(replace: at least one chart, saved in charts/ and shown here. Give it a title and labelled axes with units.)_

![TODO chart title](charts/TODO.png)

## Reflection

Reflecting on this whole process, I definetely gained a lot of respect for the systems working behind any data stream. Constantly encountering issues with security or runtime made me realize that I often take these two important components to any project for granted. I also really enjoyed seeing in realtime D3 of session 2. Seeing how a broker can down, viewing that on RedPanda dashboard and then bringing it back up was really cool. 

## Acknowledgements
I bounced ideas about code off of both Caroline and Rachel. For AI assistance, I used ChatGPT to problem-solve all of the lagging and security issues. As both of these were outside the scope of the class and provided resources, I used its suggestions and code to update my security certificate, check the speed of messages, pin-point the lags in the system and change to my computer's regular terminal. 

## All things that were labeled "Write Down" 
Checkpoint B: 
- Messages in the topic: 2,118 + 220 + 1,125 = 3,463.

