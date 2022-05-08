
## 1. Handle `TRUNCATE`

```text
[2022-05-04 17:24:55,103] INFO Handle unknown record-type = [TRUNCATE] Transaction ID [31] Sequence ID [158922174488] (laoflch.debezium.connector.informix.InformixStreamingChangeEventSource:270)
```

## 2. Late events of "ROLLBACK" and "COMMIT"

## 3. Interleaving Transaction

```
Assume : 
 - X_0 Begin of Transaction
 - X_e End of Transaction

A_0
A_1
    B_0
A_2
A_3
    B_1
    B_2
    B_e    <- offset(commit_lsn = B_e.ts)
A_4
A_5
A_6
////
A_e
```

## 4. Metric for CDCEngine?


## 5. Column Type Blacklist

- lvarchar
- SMALLFLOAT
- Decimal(x,8)

## 6. 日切

## 7. tx_id becomes commit_id

```text
$ kcat -b localhost:9092 -t informix-214414.cdctable.hello -C | jq ".payload.source.tx_id

"36"
"36"
"36"
"36"
"34"
"34"
"34"
"34"
"34"
...
"171814396820"
"171814396820"
"171814396820"
"171814396820"
"171814396820"
"171814396820"
"171814396820"
```

## 8. Docker Integration Test

## 9. Implements Metrics

- Reference: https://github.com/debezium/debezium/blob/main/debezium-connector-mysql/src/main/java/io/debezium/connector/mysql/MySqlChangeEventSourceMetricsFactory.java
