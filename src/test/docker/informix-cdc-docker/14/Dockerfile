FROM icr.io/informix/informix-developer-database:14.10.FC9W1DE

ADD --chown=informix:informix informix_post_init.sh /opt/ibm/config
ADD --chown=informix:informix testdb.sql /opt/ibm/informix/etc

RUN sed -Ei 's/^(USEOSTIME)\s+\S/\1 1/' /opt/ibm/informix/etc/onconfig.std

ENV SIZE small
ENV LICENSE accept
ENV RUN_FILE_POST_INIT informix_post_init.sh
