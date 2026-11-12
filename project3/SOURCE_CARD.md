# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name: Victoria Martinez** 
**Date: Nov 12, 2026**

### **Order of probing:** coinbase, bluesky and  opensky 
Opensky was the first one that worked but after updating my coinbase certificate, coinbase worked too and is the source I will be using for this project. 
## 1. The source
- Source and URL: Coinbase, wss://ws-feed.exchange.coinbase.com
- Method (WebSocket push, REST polling or REST history pull): WebSocket push, subscribes to the matches channel within 5 seconds
- Account, key or User-Agent needed: No account or API key needed for this public feed (which is very helpeful for collecting all 50,000 messages)
- Limits I know about (rate limits, paging, reconnects): some messages do not contain the needed product_id (99.8%  product_id   <- missing in some messages). An initial last_match is not necessarily a new trade. And lastly, messages can be dropped and the WebSocket has no replay. 

## 2. One real message
Paste one message from the probe (shorten long text fields):

--- sample message 2 ---
```
{
  "type": "last_match",
  "trade_id": 175991703,
  "maker_order_id": "e45c47f2-6bba-4f7f-9c76-5ec284a6a72e",
  "taker_order_id": "cf8cc063-00bb-4ed2-9d3c-2b6ec38a191c",
  "side": "buy",
  "size": "8245",
  "price": "0.06562",
  "product_id": "DOGE-USD",
  "sequence": 24589191479,
  "time": "2026-11-12T13:48:16.487244Z"
}

```

## 3. Volume maths
- Measured rate (from the probe):   **52.98 messages**         per **sec over 120 second collection rate**
- Live capture needed for 50,000: **943.8 seconds / 16 mins**
- History / backfill available? How many days? No WebSocket replay, so will be collecting messages live. 
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): Subscribe to BTC-USD, ETH-USD, SOL-USD, XRP-USD, DOGE-USD and
  ADA-USD. I'll run the producer for at least 45 minutes to make sure I can capture enough messages even if the trading is slower or there are interruptions. 

## 4. Topic design
- Topic name: **coinbase_trades**                       (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: **product_id**
- Why this key (ordering, spread across partitions): Trades for one product go to the same partition which perserves their order within that partition. Multiple products can distribute records across partitions; I will verify that all three receive records. 

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander) The same topic, partition and offset processed or landed twice by the lander. 
- What makes two records **the same thing** (the same trade, post, aircraft position)? When two messages represent the same Coinbase trade, their product_id and trade_id will identify that even if they have different Kafka offsets. 
- If the same thing can arrive in several versions, which one do I keep? If the same trade reappears again, I will keep one copy. But if the copies differ, I will flag the conflict for inspection to make sure which copy is correct rather than assuming that the later copy is the correct one. 

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join: How do trades per minute counts compare across different markets like the BTC-USD, ETH-USD and DOGE-USD during the capture period?

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join


### Full Message (just in case)
Connected. Reading for 120 s...
  message 1 received
  message 2 received
  message 3 received
  500 messages, 82.3/s
  1000 messages, 128.6/s
  1500 messages, 113.5/s
  2000 messages, 120.3/s
  2500 messages, 97.1/s
  3000 messages, 85.6/s
  3500 messages, 89.3/s
  4000 messages, 84.4/s
  4500 messages, 74.3/s
  5000 messages, 64.6/s
  5500 messages, 58.4/s
  6000 messages, 55.1/s

======================================================================
PROBE REPORT: coinbase
======================================================================

--- sample message 1 ---
{
  "type": "last_match",
  "trade_id": 1119588902,
  "maker_order_id": "e42b3148-8ae2-4471-8ed1-a796a9e1633b",
  "taker_order_id": "0cc44e1a-837a-4891-a07a-ee27e5a14068",
  "side": "sell",
  "size": "0.01572401",
  "price": "72500.39",
  "product_id": "BTC-USD",
  "sequence": 139214072418,
  "time": "2026-11-12T13:48:16.586379Z"
}

--- sample message 2 ---
{
  "type": "last_match",
  "trade_id": 175991703,
  "maker_order_id": "e45c47f2-6bba-4f7f-9c76-5ec284a6a72e",
  "taker_order_id": "cf8cc063-00bb-4ed2-9d3c-2b6ec38a191c",
  "side": "buy",
  "size": "8245",
  "price": "0.06562",
  "product_id": "DOGE-USD",
  "sequence": 24589191479,
  "time": "2026-11-12T13:48:16.487244Z"
}

--- sample message 3 ---
{
  "type": "subscriptions",
  "channels": [
    {
      "name": "matches",
      "product_ids": [
        "ETH-USD",
        "SOL-USD",
        "XRP-USD",
        "DOGE-USD",
        "ADA-USD",
        "BTC-USD"
      ],
      "account_ids": null
    }
  ]
}

--- fields in the first 500 messages (how often each appears) ---
  100.0%  type
   99.8%  maker_order_id   <- missing in some messages
   99.8%  price   <- missing in some messages
   99.8%  product_id   <- missing in some messages
   99.8%  sequence   <- missing in some messages
   99.8%  side   <- missing in some messages
   99.8%  size   <- missing in some messages
   99.8%  taker_order_id   <- missing in some messages
   99.8%  time   <- missing in some messages
   99.8%  trade_id   <- missing in some messages
    0.2%  channels   <- missing in some messages

--- volume ---
  messages: 6358   not JSON: 0
  rate: 52.98 per second
  50,000 records need about 943.8 seconds (16 min)

--- for your Source Design Card ---
  protocol  WebSocket (push). Public market data; you must subscribe within 5 seconds of connecting.
  key       product_id (every trade of one product in order)
  identity  product_id + trade_id. sequence numbers are per product: a gap means a dropped message.
  catches   The first message is 'subscriptions', then one 'last_match' per product: not new trades. Coinbase warns messages can be dropped from this channel. Rate depends on the market (quiet at night).
  history   No replay on the WebSocket. Collect live, starting on the evening of Session 1; add products to go faster.
  questions Trades per minute vs. price moves? Buy vs. sell pressure? Trade size distribution by product?
