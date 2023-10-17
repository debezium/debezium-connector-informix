FROM icr.io/informix/informix-developer-database:12.10.FC12W1DE

ADD --chown=informix:informix informix_init.sh /opt/ibm/scripts

ENV TYPE oltp
ENV SIZE medium
ENV LICENSE accept
ENV RUN_FILE_POST_INIT informix_post_init.sh
