FROM icr.io/informix/informix-developer-database:14.10.FC9W1DE

ADD --chown=informix:informix informix_pre_init.sh /opt/ibm/config
ADD --chown=informix:informix informix_post_init.sh /opt/ibm/config
ADD --chown=informix:informix informix_config.custom /opt/ibm/data
ADD --chown=informix:informix ../testdb.sql /opt/ibm/data

ENV LICENSE=accept
ENV SIZE=medium
ENV DB_SBSPACE=1
ENV RUN_FILE_PRE_INIT=informix_pre_init.sh
ENV RUN_FILE_POST_INIT=informix_post_init.sh
