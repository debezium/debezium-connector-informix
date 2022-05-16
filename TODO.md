
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

## 8. [X] Docker Integration Test

## 9. Implements Metrics

- Reference: https://github.com/debezium/debezium/blob/main/debezium-connector-mysql/src/main/java/io/debezium/connector/mysql/MySqlChangeEventSourceMetricsFactory.java

## 10. Testcases

- [ ] `.with(SNAPSHOT_MODE, INITIAL)`
- [ ] More Types: bigint, bigserial
- [ ] `update clause`

## 11. decimal handling mode

If we set "decimal.handling.mode=percision", which is the default option, it will cause the following exception:

```text
[2022-05-15 00:48:42,552] ERROR WorkerSourceTask{id=informix-connector-214414-0} Task threw an uncaught and unrecoverable exception (org.apache.kafka.connect.runtime.WorkerTask:179)
org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndHandleError(RetryWithToleranceOperator.java:178)
	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execute(RetryWithToleranceOperator.java:104)
	at org.apache.kafka.connect.runtime.WorkerSourceTask.convertTransformedRecord(WorkerSourceTask.java:290)
	at org.apache.kafka.connect.runtime.WorkerSourceTask.sendRecords(WorkerSourceTask.java:316)
	at org.apache.kafka.connect.runtime.WorkerSourceTask.execute(WorkerSourceTask.java:240)
	at org.apache.kafka.connect.runtime.WorkerTask.doRun(WorkerTask.java:177)
	at org.apache.kafka.connect.runtime.WorkerTask.run(WorkerTask.java:227)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	at java.base/java.lang.Thread.run(Thread.java:833)
Caused by: org.apache.kafka.connect.errors.DataException: BigDecimal has mismatching scale value for given Decimal schema
	at org.apache.kafka.connect.data.Decimal.fromLogical(Decimal.java:68)
	at org.apache.kafka.connect.json.JsonConverter$13.toJson(JsonConverter.java:206)
	at org.apache.kafka.connect.json.JsonConverter.convertToJson(JsonConverter.java:606)
	at org.apache.kafka.connect.json.JsonConverter.convertToJson(JsonConverter.java:693)
	at org.apache.kafka.connect.json.JsonConverter.convertToJson(JsonConverter.java:693)
	at org.apache.kafka.connect.json.JsonConverter.convertToJsonWithEnvelope(JsonConverter.java:581)
	at org.apache.kafka.connect.json.JsonConverter.fromConnectData(JsonConverter.java:335)
	at org.apache.kafka.connect.storage.Converter.fromConnectData(Converter.java:62)
	at org.apache.kafka.connect.runtime.WorkerSourceTask.lambda$convertTransformedRecord$2(WorkerSourceTask.java:290)
	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndRetry(RetryWithToleranceOperator.java:128)
	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndHandleError(RetryWithToleranceOperator.java:162)
	... 11 more
```

## 12. `transaction.total_order` is always same as `data_collection_order`, when enabled connector option `provide.transaction.metadata`

