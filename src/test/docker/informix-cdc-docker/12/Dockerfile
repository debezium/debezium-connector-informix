FROM icr.io/informix/informix-developer-database:12.10.FC12W1DE

ADD --chown=informix:informix informix_init.sh /opt/ibm/scripts
ADD --chown=informix:informix sch_init_informix.custom.sql /opt/ibm/data
ADD --chown=informix:informix informix_config.custom /opt/ibm/data
ADD --chown=informix:informix ../testdb.sql /opt/ibm/data

ENV LICENSE=accept
ENV SIZE=custom
ENV DB_SBSPACE=1
