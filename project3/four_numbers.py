Session 2 · checkpoint C · deck step 5 of 8
editor
T3 numbers
T4 rpk · docker · git
Count what landed: the four numbers
ds3022-streaming · Visual Studio Code
EXPLORER
▾ DS3022-STREAMING
▸
.venv
▾
project3
▸
data
▸
docs
▸
kit
·
.gitignore
md
ADAPTING.md
md
BRIEF.md
md
CHAOS_CARDS.md
md
CHECKPOINTS.md
py
flow.py
py
four_numbers.py
py
lander.py
py
probe.py
py
producer.py
md
PROGRESS.md
md
README.md
md
RUBRIC.md
md
SOURCE_CARD.md
md
SOURCES.md
·
.gitignore
y
docker-compose.yml
py
quakes_flow.py
py
quakes_lander.py
py
quakes_producer.py
md
README.md
t
requirements.txt
py
seismic-consumer.py
py
seismic-producer.py
py
stream-kafka.py
py
stream-seismic.py
py
weather-consumer.py
py
weather-producer.py
four_numbers.py
# four_numbers.py: the four numbers from data/raw, until flow.py is ready
import sys
import duckdb
if len(sys.argv) < 2:
    sys.exit("Give your id column(s), e.g.  python four_numbers.py product_id,trade_id")
IDS = sys.argv[1].split(",")
RAW, OK = "read_parquet('data/raw/*.parquet')", "action <> 'malformed'"
HAS_ID = " AND ".join(f"{c} IS NOT NULL" for c in IDS)
q = duckdb.connect().execute   # in memory: reads the files, writes nothing
try:
    raw, bad = q(f"SELECT count(*), count(*) FILTER (WHERE action = 'malformed') FROM {RAW}").fetchone()
    records = q(f"SELECT count(*) FROM (SELECT DISTINCT kafka_partition, kafka_offset FROM {RAW} WHERE {OK})").fetchone()[0]
    things = q(f"SELECT count(*) FROM (SELECT DISTINCT {', '.join(IDS)} FROM {RAW} WHERE {OK} AND {HAS_ID})").fetchone()[0]
except duckdb.Error as e:
    sys.exit(f"{e}\nCheck: run it from project3, at least one file landed, id names as in your SCHEMA.")
print(f"raw {raw:,} | malformed {bad:,} | unique Kafka records {records:,} | unique {'+'.join(IDS)} {things:,}")