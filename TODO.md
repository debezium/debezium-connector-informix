## 1. Fix reload of schema after restart

## 2. Fix blocking snapshot

## 3. Successfully determine current maximum LSN

## 4. Testcases

Adapt more standard testcases from Debezium parent

## 5. Implements Metrics

- Reference: https://github.com/debezium/debezium/blob/main/debezium-connector-mysql/src/main/java/io/debezium/connector/mysql/MySqlChangeEventSourceMetricsFactory.java

## 6. Decimal handling mode

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
