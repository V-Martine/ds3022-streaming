# docs/

Evidence the grader looks at. Save screenshots here (PNG or JPG) and show or link them from your README.

Required (at least these three):

1. `topic.png`: Redpanda Console (`http://127.0.0.1:8080`) showing your topic with 3 partitions and its message count (checkpoint B).
2. `flow_log.png`: the terminal output of your flow on the full data, with the four numbers (checkpoint E).
3. `prefect_dashboard.png`: a completed flow run in the Prefect dashboard (checkpoint E).

Useful extras: the architecture diagram, before/after numbers from the chaos round, the chart before and after the chart swap.

### Victoria's Running Notes: 
Checkpoint B — Bluesky:
- Topic: bluesky_posts
- Three partitions, each replicated across brokers 0, 1, and 2.
- Messages at verification: 12,289 + 12,616 + 13,299 = 38,204.
- All three log-start offsets were 0.

flow_log: 
(.venv) (base) papi@Victoria-MacBook-7 project3 % python four_numbers.py did,rkey
raw 54,612 | malformed 0 | unique Kafka records 54,612 | unique did+rkey 53,956