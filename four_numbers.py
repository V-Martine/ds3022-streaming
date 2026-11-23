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
