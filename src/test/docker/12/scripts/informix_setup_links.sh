#!/bin/bash
#
#  name:        informix_setup_links.sh:
#  description: Setup tmp files for sqlhosts and onconfig 
#  Called by:   informix_entry.sh


## sqlhosts to get recreated each start/run due to hostname changes
#cp $INFORMIX_DATA_DIR/tmp/sqlhosts $INFORMIXDIR/etc/sqlhosts
#cp $INFORMIX_DATA_DIR/tmp/$ONCONFIG $INFORMIXDIR/etc/$ONCONFIG


if [ ! -e $INFORMIXDIR/etc/$ONCONFIG ]
then
 ln -s $INFORMIX_DATA_DIR/links/onconfig $INFORMIXDIR/etc/$ONCONFIG
fi

if [ ! -e $INFORMIXDIR/etc/sqlhosts ]
then
 ln -s $INFORMIX_DATA_DIR/links/sqlhosts $INFORMIXDIR/etc/sqlhosts
fi