FROM icr.io/informix/informix-developer-database:12.10.FC12W1DE

ADD --chown=informix:informix informix_init.sh /opt/ibm/scripts

RUN sed -Ei 's/^(USEOSTIME)\s+\S/\1 1/' /opt/ibm/informix/etc/onconfig.std

ENV SIZE small
ENV LICENSE accept
