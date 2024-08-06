## ~~1. Fix reload of schema after restart~~
## ~~2. Fix blocking snapshot~~
## ~~3. Successfully determine current maximum LSN~~
## ~~4. Testcases~~
~~Adapt more standard testcases from Debezium parent~~

## 5. Implements Metrics

- Reference: https://github.com/debezium/debezium/blob/main/debezium-connector-mysql/src/main/java/io/debezium/connector/mysql/MySqlChangeEventSourceMetricsFactory.java

## ~~6. Decimal handling mode~~
~~If we set "decimal.handling.mode=percision", which is the default option, it will cause the following exception: ...~~
