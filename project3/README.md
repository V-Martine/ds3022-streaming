# BlueSky Steaming Pipeline Project - DS3022

**Author:** Victoria
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
| D1 Crash between file and offsets | TODO | TODO | TODO | TODO |
| D2 Poison message | TODO | TODO | TODO | TODO |
| TODO (D3, D4 or D5) | TODO | TODO | TODO | TODO |

## The four numbers

From the flow log on the full data (screenshot in `docs/`):

| Number | Full data | Sample (`data/sample`) |
|---|---:|---:|
| Raw records landed | TODO | TODO |
| Malformed (kept in raw) | TODO | TODO |
| Unique Kafka records | TODO | TODO |
| Unique TODO (your entity) | TODO | TODO |

## Challenges / Obstacles

_(replace: what this source made hard in gathering, processing and analysis, and how you worked through it. What methods and tools did you use?)_

## Analysis

_(replace: one clear paragraph. Your research question, what the data shows, one finding that surprised you or is not obvious, and one limitation of your data or method.)_

## Plot / Visualization

_(replace: at least one chart, saved in charts/ and shown here. Give it a title and labelled axes with units.)_

![TODO chart title](charts/TODO.png)

## Reflection

_(replace: about 150 words, first person. What did you learn about streaming systems that you did not know before Thu 22 Oct? What would you do differently if you started again?)_

## Acknowledgements

_(replace: everyone who helped you and how (classmates, instructor, online sources), code you reused (for example connection code from probe.py), and any AI assistance: what for, and how you checked the result. Write "None" if there is nothing to declare.)_


## All things that were labeled "Write Down" 
Checkpoint B: 
- Messages in the topic: 2,118 + 220 + 1,125 = 3,463.

