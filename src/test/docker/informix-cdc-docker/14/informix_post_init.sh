#!/bin/bash

dbaccess < $INFORMIXDIR/etc/syscdcv1.sql

dbaccess < $INFORMIXDIR/etc/testdb.sql

# cat /dev/null > $INFORMIX_DATA_DIR/spaces/rootdbs.001 > $INFORMIX_DATA_DIR/spaces/rootdbs.002
# chmod 660 $INFORMIX_DATA_DIR/spaces/rootdbs.001 $INFORMIX_DATA_DIR/spaces/rootdbs.002
# onspaces -c -d rootdbs1 -p $INFORMIX_DATA_DIR/spaces/rootdbs.001 -o 0 -s 350000
# onspaces -c -d rootdbs2 -p $INFORMIX_DATA_DIR/spaces/rootdbs.002 -o 0 -s 350000

