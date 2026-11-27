# BlueSky Steaming Pipeline Project - DS3022

**Author:** Victoria Martinez

**DS 3022 Data Project 3, Fall 2026**

## Data source
This project uses Bluesky Jetstream, a push-based source accessed through a WebSocket connection. The producer sends each received event to Kafka for downstream processing. Jetstream does not require a subscription message after connecting `SUBSCRIBE = NONE`. I ran with a 120-second probe which received 5,516 messages in that time. 

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
git clone https://github.com/vgm6/ds3022-streaming.git
cd ds3022-streaming
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

### 3. Start the cluster and create the live topic 
- Begin by opening up 4 seperate terminals and make sure that Docker Desktop is loaded and working. 
- Ensure you run each script in a different terminal

```bash
docker compose up -d
docker exec -it redpanda-0 rpk cluster info
docker exec -it redpanda-0 rpk topic create bluesky_posts -p 3 -r 3
docker exec -it redpanda-0 rpk topic describe bluesky_posts -p 
```
Once the topic is created, there is no need to recreate. 

### 4. Collect and land the live data 
- In terminals 1 and 2, run producer and lander respectively. 

**Terminal 1 - run producer**
```bash
source .venv/bin/activate
cd project3
python -u producer.py
```
- Once all of the data has been collected, you can stop the producer with Ctrl+C 

**Terminal 2 - lander**
```bash
source .venv/bin/activate
cd project3
python -u lander.py
```
- Once the producer has been stopped, wait until the lander catches us and writes any final partial batch and then stop with Ctrl+C as well. 

### 5. Running the flow on the full set of data
- In terminal 3, start the virtual environment and run flow.py 
```bash
source .venv/bin/activate
cd project3
python flow.py
python flow.py
```
- Keep the lander stopped so both of the runs see the same files. 
- On my Mac, I had enough files to hit the shell's open-file limit. If you see that, run `ulimit -Sn 4096` to fix the issue. I also did have to clear several GB of storage on my computer in order for DuckDB to run properly. 

## Architecture
![Architecture Diagram](docs/arch.png)
The full run writes to `bluesky.duckdb`. The sample runs the same tasks against `data/sample/`, with a separate database and chart.


## Design decisions
1. **Key and partitions.**  I used the provided `did` as the Kafka key so the events that belong to the same account all go to the same partition under the three-partition set-up we worked on in class. While Kafka preserves record order within a partition, it doesn't extend to the whole topic. I decided against using a random key as that could scatter one post's events of creating, updating, deleling, etc across multiple partitions which defeats the partition ordering we want to perserve. 
2. **Batch size and time.** My 120-second probe received 5,516 messages which equals a 45.95 messages/second for the probe rate and not the exact rate of post commits sent to Kafka. The lander writes a batch of either 1,000 records or after 60 seconds, depending on which one comes first. Because of the ~46 messages/sec rate that my probe received, 1000 messages would arrive in approx. 22 seconds so I'm expecting the size rule to close most of the live batches. That works out to around 165 files/hour although, as I constantly experienced changes in traffic or filtering issues could affect the actual rate and count. 
3. **Offsets after the file.** _The lander writes a temporary Parquet file, renames it to the final filename, and then stores offsets. If it crashes after the rename, but before the offsets are committed, it can reread records because the rows already exist on the disk memory. Because the flow can remove them, I accept those extra copies. Committing first could cause the lander to skip records that were never saved.
4. **Duplicates.** Within `bluesky_posts`, a Kafka record is identified by `(kafka_partition, kafka_offset)`. So two records that have the same partition and offest will make them the same thing. In `stage()`, `SELECT DISTINCT * EXCLUDE (landed_at)` removes replayed copies that differ only in landing time. In `entities()`, `ROW_NUMBER()` partitioned by `(did, rkey)` keeps the latest observed operation by Jetstream `time_us`, using Kafka metadata to break ties.
5. **Windows and time.** Rather than grouping by when files landed, I chose to convert Jetstream's `time_us` to a timestamp and group it into one-hour UTC windows. This matters because the lander can write a backlog much later than the events occurred. The chart counts only posts whose latest observed operation is `create` and uses the first declared language; partially observed hours are not complete hourly rates.
6. **Ten times the volume.** I would expect the Parquet-to-DuckDB stage to run into storage and file-handling limits first. Because of the amount of files I had stored, the full run already hit an open-file limit and ran out of disk space while writing staging. At ten times the rate, I would compact the small Parquet batches into larger files and process new records incrementally instead of rebuilding every table from all the raw data on every run.
## Chaos round

| Card | Prediction | What I did | Numbers before → after | Explanation |
|---|---|---|---|---|
| D1 Crash between file and offsets | I predict that the lander will try to read the crashed batch again because the offsets were not stored properly. | Kept the producer stopped and used the existing backlog. Ran `"$TEST_PY" -u lander.py crash`, then ran `"$TEST_PY" -u lander.py once`, making sure to count between each run. The restart wrote one file before hitting a Python scope error. | Raw 54,612 → 56,612; malformed 0 → 0; unique Kafka records 54,612 → 55,780; unique posts 53,956 → 55,122. | The intentional crash left a Parquet file on disk without storing its offsets. The restart wrote 832 replayed records and 168 not previously landed. The replayed copies increased the raw count but not the unique Kafka-record count. |
| D2 Poison message | I predict that the non-JSON message with be recorded with both a raw row and a malformed row but not stop the lander.| I first created the test topic in T4 and then in T2, started the test lander. After following the instructions to send one malformed message in T4, I verified the result in T2 and T3. Between this and the previous test, I had something weird happen with my lander/producer and somehow acquired over a million messages hence the number difference (I spoke to Prof. Pons about this in class and had to raise the soft limit in the terminal to counter the huge increse in the numbers).| Numbers changed from: raw 1,462,631 → 1,462,632 ,  malformed 0 → 1, unique Kafka records and unique did+rkey stayed the same. 
 |  The parser caught the JSON error and saved the original text in one raw row marked `malformed`. The row stayed on disk for inspection but was excluded from the valid-record and post counts. | |
| D3 Broker Down | D3: I predict that the broker 1 will go down and as leader. However, high-watermarks will not increase | With the producer and lander running, I stopped broker 1, checked the partition offsets twice during the outage, and then brought the broker back up. | My prediction that high-watermakrs would not increase was incorrect, as when I tested two different times while broker 1 was still down, the high-watermakers number increased showing that the topic is still accepting records during the outage. Numbers changed from 1,462,632 → 1,487,245, unique Kafka records 1,461,631 → 1,486,244, unique did+rkey 1,433,497 →  1,457,870| My prediction about the high-watermarks was wrong: they increased while broker 1 was down. With three replicas per partition, the two remaining brokers could form a majority and keep accepting writes after leadership moved. Raw rows and unique Kafka records both increased by 24,613, so the counts show no additional replay copies in this interval. |

Table showing the differences between each step in the chaos round
| Chaos Round | Raw | Malformed| Unique Kafka Records | Unique did+rkey |
|---|---:|---:|---:|---:|
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
I asked how observed Bluesky post counts varied by language across hourly UTC windows. English seemed to be the clear winner in terms of quantity across various hours. Also between the hours, there was a higher total count in the early hours of the morning rather than later in the day. This was one result I did not expect as I thought later in the day there were be more messages flowing. These are counts for the periods I actually captured, not estimates of platform-wide posting rates: some hours were only partially observed, and the chart counts only posts whose latest observed operation was `create`. It also uses only the first declared language rather than detecting the language from the text.

## Plot / Visualization

![Posts by the Hour Organized by Language](charts/posts_by_hour_language.png)
Each bar shows an observed UTC hour. The vertical axis is a post count, and the legend shows at most four individual categories plus “Other languages.” Hours with no plotted records are omitted, so the spacing between bars does not show elapsed time.

## Reflection

Reflecting on this whole process, I definetely gained a lot of respect for the systems working behind any data stream. Constantly encountering issues with security or runtime made me realize that I often take these two important components to any project for granted. In D1, a file existed on disk even though the consumer had not saved its progress. When I restarted the lander, I could actually count the repeated records and see why writing a file and recording an offset are two different operations. I also really enjoyed seeing in realtime D3 of session 2. Seeing how a broker can down, viewing that on RedPanda dashboard and then bringing it back up was really cool. 
If I started again, I would test each stage with a small batch before collecting more data. I would also check import times and available disk space earlier, and monitor the consumer lag so I know how much data is still waiting to be landed.

## Acknowledgements
I bounced ideas about code off of both Caroline and Rachel. For AI assistance, I used ChatGPT to problem-solve all of the lagging and security issues. As both of these were outside the scope of the class and provided resources, I used its suggestions and code to update my security certificate, check the speed of messages, pin-point the lags in the system and change to my computer's regular terminal. I checked the suggestions against actual probe output, timed imports, Kafka partition offsets, and DuckDB counts.

## All things that were labeled "Write Down" 
- **Checkpoint B — Bluesky:** 12,289 + 12,616 + 13,299 = **38,204** messages at the recorded checkpoint, with a log-start offset of zero on each partition.
- **Checkpoint D:** Before-and-after counts are recorded in the Chaos round table.
- **Checkpoint E:** Full-data and sample counts are recorded in The four numbers.

