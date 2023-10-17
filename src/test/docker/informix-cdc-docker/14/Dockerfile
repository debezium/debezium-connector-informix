FROM icr.io/informix/informix-developer-database:14.10.FC9W1DE

ADD --chown=informix:informix informix_post_init.sh /opt/ibm/config
ADD --chown=informix:informix testdb.sql /opt/ibm/informix/etc

ENV TYPE oltp
ENV SIZE medium
ENV LICENSE accept
ENV RUN_FILE_POST_INIT informix_post_init.sh
