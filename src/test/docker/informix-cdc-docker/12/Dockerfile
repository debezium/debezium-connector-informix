FROM icr.io/informix/informix-developer-database:12.10.FC12W1DE

ADD --chown=informix:informix informix_init.sh /opt/ibm/scripts

ENV SIZE small
ENV LICENSE accept
ENV RUN_FILE_POST_INIT informix_post_init.sh
