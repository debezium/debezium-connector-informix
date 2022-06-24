
# Notes on how to handle Schema Changes

1. syscolumns:
2. systables:
3. sysfragments:

## case 1:

```sql
create table tbl_1(a INT, b STRING);
```
 
// begin txn
// insert syscolumns: a, INT
// insert syscolumns: b STRING
// insert systables: tbl_1
// commit

-- > handle it by caling {@link EventDispatcher#dispatchSchemaChangeEvent}

## case 2:

```text
rename table bbtemp to bb;
```

// begin txn
// update (name = bbtemp) ==> (name = bb)
// commit

-- > handle it by calling {@link EventDispatcher#dispatchSchemaChangeEvent}

## case 3: 日切

```sql
alter fragment on table bbhis attach bb as partition bbhis date = '2022-06-01';
```

// begin txn
// insert sysfragments values ( xxx )
// commit

-- > restart IfxCDCEngine() at current LSN
