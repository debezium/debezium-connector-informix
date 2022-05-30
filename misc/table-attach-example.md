
# Notes on Table attach of informix database

## Hello

```sql
drop table informix.bbhis;
drop table bb;

create table tak:informix.bbhis(id integer NOT NULL)
fragment by expression
  (id < 20) in rootdbs1,
  (id >= 20) in rootdbs2;
  
create table bb(id integer NOT NULL);

insert into bb values (1);

execute function syscdcv1:informix.cdc_set_fullrowlogging('tak:informix.bb', 0);
alter fragment on table informix.bbhis attach informix.bb as partition bbhis5 id < 20;

select flags into $values from informix.systables where tabname = 'bb';

select * from informix.sysfragments;
```


```text
touch $INFORMIXDIR/dbs/rootdbs1 $INFORMIXDIR/dbs/rootdbs2 $INFORMIXDIR/dbs/rootdbs3 $INFORMIXDIR/dbs/rootdbs4
touch $INFORMIXDIR/dbs/workdbs1 $INFORMIXDIR/dbs/workdbs2 $INFORMIXDIR/dbs/workdbs3 $INFORMIXDIR/dbs/workdbs4 $INFORMIXDIR/dbs/workdbs5 $INFORMIXDIR/dbs/workdbs6

chmod 660 $INFORMIXDIR/dbs/*dbs*

onspaces -c -d logdbs -p $INFORMIXDIR/dbs/logdbs -o 40 -s 1001000
onspaces -c -d phydbs -p $INFORMIXDIR/dbs/phydbs -o 40 -s 1000000
onspaces -c -d tempdbs -p $INFORMIXDIR/dbs/tempdbs -o 40 -s 500000
onspaces -c -d workdbs1 -p $INFORMIXDIR/dbs/workdbs1 -o 40 -s 2048000

onspaces -a rootdbs -p $INFORMIXDIR/dbs/rootdbs1 -o 60 -s 2048000
onspaces -a rootdbs -p $INFORMIXDIR/dbs/rootdbs2 -o 80 -s 2048000
onspaces -a rootdbs -p $INFORMIXDIR/dbs/rootdbs3 -o 100 -s 2048000
onspaces -a rootdbs -p $INFORMIXDIR/dbs/rootdbs4 -o 120 -s 2048000

onspaces -c -d workdbs2 -p $INFORMIXDIR/dbs/workdbs2 -o 40 -s 2048000
onspaces -c -d workdbs3 -p $INFORMIXDIR/dbs/workdbs3 -o 40 -s 2048000
onspaces -c -d workdbs4 -p $INFORMIXDIR/dbs/workdbs4 -o 40 -s 2048000

onspaces -c -d rootdbs1 -p $INFORMIXDIR/dbs/rootdbs1 -o 40 -s 2048000
onspaces -c -d rootdbs2 -p $INFORMIXDIR/dbs/rootdbs2 -o 40 -s 2048000
onspaces -c -d rootdbs3 -p $INFORMIXDIR/dbs/rootdbs3 -o 40 -s 2048000
```

## On docker environment

```shell
touch $INFORMIX_DATA_DIR/spaces/rootdbs1
touch $INFORMIX_DATA_DIR/spaces/rootdbs2
touch $INFORMIX_DATA_DIR/spaces/rootdbs3
touch $INFORMIX_DATA_DIR/spaces/rootdbs4

chmod 660 $INFORMIX_DATA_DIR/spaces/rootdbs1
chmod 660 $INFORMIX_DATA_DIR/spaces/rootdbs2
chmod 660 $INFORMIX_DATA_DIR/spaces/rootdbs3
chmod 660 $INFORMIX_DATA_DIR/spaces/rootdbs4

onspaces -c -d rootdbs1 -p $INFORMIX_DATA_DIR/spaces/rootdbs1 -o 60 -s 10240
onspaces -c -d rootdbs2 -p $INFORMIX_DATA_DIR/spaces/rootdbs2 -o 60 -s 10240
onspaces -c -d rootdbs3 -p $INFORMIX_DATA_DIR/spaces/rootdbs3 -o 60 -s 10240
onspaces -c -d rootdbs4 -p $INFORMIX_DATA_DIR/spaces/rootdbs4 -o 60 -s 10240

# show dbspaces
onstat -d
```

```sql
DATABASE testdb;

DROP TABLE IF EXISTS testdb:informix.bbhis;
DROP TABLE IF EXISTS testdb:informix.bb;

CREATE TABLE testdb:informix.bbhis(id INTEGER NOT NULL)
    FRAGEMENT BY EXPRESSION
(id < 20) IN rootdbs1,
(id >= 20) IN rootdbs2;


CREATE TABLE testdb:informix.bb(id INTEGER NOT NULL);

INSERT INTO testdb:informix.bb VALUES (1);
INSERT INTO testdb:informix.bb VALUES (2);
INSERT INTO testdb:informix.bb VALUES (3);
INSERT INTO testdb:informix.bb VALUES (4);
INSERT INTO testdb:informix.bb VALUES (5);
INSERT INTO testdb:informix.bb VALUES (6);
INSERT INTO testdb:informix.bb VALUES (7);
INSERT INTO testdb:informix.bb VALUES (8);
INSERT INTO testdb:informix.bb VALUES (9);

INSERT INTO testdb:informix.bb VALUES (10);
INSERT INTO testdb:informix.bb VALUES (11);
INSERT INTO testdb:informix.bb VALUES (12);
INSERT INTO testdb:informix.bb VALUES (13);
INSERT INTO testdb:informix.bb VALUES (14);
INSERT INTO testdb:informix.bb VALUES (15);
INSERT INTO testdb:informix.bb VALUES (16);
INSERT INTO testdb:informix.bb VALUES (17);
INSERT INTO testdb:informix.bb VALUES (18);
INSERT INTO testdb:informix.bb VALUES (19);

INSERT INTO testdb:informix.bb VALUES (20);
INSERT INTO testdb:informix.bb VALUES (21);
INSERT INTO testdb:informix.bb VALUES (22);
INSERT INTO testdb:informix.bb VALUES (23);
INSERT INTO testdb:informix.bb VALUES (24);
INSERT INTO testdb:informix.bb VALUES (25);
INSERT INTO testdb:informix.bb VALUES (26);
INSERT INTO testdb:informix.bb VALUES (27);
INSERT INTO testdb:informix.bb VALUES (28);
INSERT INTO testdb:informix.bb VALUES (29);


SELECT * FROM bb;
SELECT * FROM bbhis;

--
-- kill informix-connector, then run the following:
--
EXECUTE FUNCTION syscdcv1:informix.cdc_set_fullrowlogging('testdb:informix.bb', 0);
ALTER FRAGMENT ON TABLE testdb:informix.bbhis ATTACH testdb:informix.bb AS PARTITION bbhis6 id < 10;

SELECT * FROM testdb:informix.sysfragments;
SELECT flags FROM testdb:informix.systables WHERE tabname = 'bbhis';
```
