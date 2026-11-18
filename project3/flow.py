"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.
    Run from project3/ with:
    python flow.py
    python flow.py data/sample
"""
# import statements
import os
import sys
import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

# Full data and sample use separate databases and charts
SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))
TAG = "" if NAME == "raw" else f"_{NAME}"

DB = f"bluesky{TAG}.duckdb"
CHART = f"charts/posts_by_hour_language{TAG}.png"


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    """Load all Parquet files into a DuckDB table, replacing any existing table.
    Return the number of records loaded."""
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of quakes.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_posts AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_posts").fetchone()[0]


@task
def clean():
    with duckdb.connect(DB) as con:
        # 1. The same Kafka record landed twice (at-least-once): keep one copy and 
        # landed_at differs, so exclude it before applying DISTINCT
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_posts
            WHERE action <> 'malformed' AND unid IS NOT NULL
            AND did IS NOT NULL
            AND rkey IS NOT NULL
            AND kind = 'commit'
        """)
        # 2. The same earthquake sent several times (create, updates): keep the latest.
        #    Same key -> same partition -> a higher offset is a later version.
        malformed = con.execute("""
            SELECT count(*)
            FROM raw_posts
            WHERE action = 'malformed'
        """).fetchone()[0]

        records = con.execute("""
            SELECT count(*) FROM stg_records
        """).fetchone()[0]

        return {
            "malformed": malformed,
            "records": records,
        }

# keep the lastest observed operation for each unique (did, rkey) pair, 
# which is the primary key of a Bluesky record
@task
def entities():
    """Keep the latest observed operation for each (did, rkey)."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts AS
            SELECT
                did,
                rkey,
                operation,
                make_timestamp(time_us) AS event_time,
                lang,
                text,
                (
                    raw::JSON
                    ->> '$.data.commit.record.reply.parent.uri'
                ) IS NOT NULL AS is_reply,
                kafka_partition,
                kafka_offset
            FROM stg_records
            QUALIFY row_number() OVER (
                PARTITION BY did, rkey
                ORDER BY
                    time_us DESC NULLS LAST,
                    kafka_offset DESC,
                    kafka_partition DESC
            ) = 1
        """)

        return con.execute(
            "SELECT count(*) FROM posts"
        ).fetchone()[0]


# counts the retained by the first language in hourly UTC windows
@task
def marts():
    """Count retained creates by first language in hourly UTC windows."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts_by_hour_language AS
            SELECT
                time_bucket(
                    INTERVAL '1 hour', event_time
                ) AS window_start,
                COALESCE(NULLIF(lang, ''), 'unknown') AS language,
                count(*) AS post_count
            FROM posts
            WHERE operation = 'create'
              AND event_time IS NOT NULL
            GROUP BY 1, 2
        """)

        return con.execute("""
            SELECT count(*) FROM posts_by_hour_language
        """).fetchone()[0]

# edited chart for language 
@task
def chart():
    """Plot up to four language categories plus an 'other' category."""
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("""
            SELECT window_start, language, post_count
            FROM posts_by_hour_language
            ORDER BY window_start, language
        """).fetchall()

    if not rows:
        raise ValueError(
            "No retained create operations with valid times to plot."
        )

    # Rank languages across the entire input dataset.
    totals = {}
    for _, language, count in rows:
        totals[language] = totals.get(language, 0) + count

    top_languages = sorted(
        totals,
        key=lambda language: (-totals[language], language),
    )[:4]

    windows = sorted({window for window, _, _ in rows})
    window_index = {
        window: index for index, window in enumerate(windows)
    }

    # At most four language categories plus "Other languages".
    series = {
        language: [0] * len(windows)
        for language in top_languages
    }

    other_label = "Other languages"
    if any(language not in top_languages for language in totals):
        series[other_label] = [0] * len(windows)

    for window, language, count in rows:
        group = (
            language
            if language in top_languages
            else other_label
        )
        series[group][window_index[window]] += count

    os.makedirs("charts", exist_ok=True)

    fig, ax = plt.subplots(figsize=(12, 5))
    x = list(range(len(windows)))
    bottom = [0] * len(windows)
    colors = ["#0072B2", "#E69F00", "#009E73", "#CC79A7", "#999999"]

    for (language, counts), color in zip(series.items(), colors):
        ax.bar(
            x,
            counts,
            bottom=bottom,
            label=language,
            color=color,
            width=0.8,
        )

        bottom = [
            current + count
            for current, count in zip(bottom, counts)
        ]

    # Show actual hour labels rather than numeric bar positions.
    step = max(1, (len(windows) + 11) // 12)
    ticks = list(range(0, len(windows), step))

    ax.set_xticks(ticks)
    ax.set_xticklabels(
        [windows[i].strftime("%d %b %Y %H:%M") for i in ticks],
        rotation=45,
        ha="right",
    )

    ax.set_title(
        "Observed Bluesky posts by language in hourly windows"
    )
    ax.set_xlabel("Hour start (UTC)")
    ax.set_ylabel("Observed posts per hourly window (count)")
    ax.legend(title="First language code")
    ax.set_axisbelow(True)
    ax.grid(axis="y", alpha=0.25)

    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(CHART, dpi=120)
    plt.close(fig)

    return CHART


@flow(name="bluesky-pipeline")
def bluesky_pipeline():
    log = get_run_logger()

    raw_count = load_raw()
    counts = stage()
    post_count = entities()
    mart_rows = marts()
    chart_path = chart()

    log.info("Raw records landed:        %d", raw_count)
    log.info("Malformed (kept in raw):   %d", counts["malformed"])
    log.info("Unique Kafka records:      %d", counts["records"])
    log.info("Unique posts:              %d", post_count)
    log.info("Mart rows:                 %d", mart_rows)
    log.info("Database: %s", DB)
    log.info("Chart: %s", chart_path)


if __name__ == "__main__":
    bluesky_pipeline()
