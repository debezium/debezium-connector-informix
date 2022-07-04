
# Building an Informix CDC Docker Image for Testing

## Overview

This will build a docker image for integration testing for Informix 12. And this image have
these characteristic:

- Local Data Storage. Since the [Informix 12's base image](https://hub.docker.com/layers/informix-developer-database/ibmcom/informix-developer-database/12.10.FC12W1DE/images/sha256-da8e2f94f8897105ae463feb2465efd74c5879184f323061b4ac3a4b54d829ee?context=explore) does not support `-e STORAGE=local` option, we have to overwrite `$INFORMIX_DATA_DIR` from `informix_inf.env`.
- A logging enabled database, named `testdb`.

### First, build base image with local data storage.

```sh
$ ./build
```

### Then, manually add cdc related features.

```text
$ docker run -it --name ifx --privileged -e SIZE=small \
    -p 9088:9088      \
    -p 9089:9089      \
    -p 27017:27017    \
    -p 27018:27018    \
    -p 27883:27883    \
    -e LICENSE=accept \
    xiaolin/ifx12-localdata-base:v1
```

Then, we switch to another shell:

```text
$ docker exec -it ifx /bin/bash

################# Enter into the container ####################
$ dbaccess - -

> create database testdb with log;
Database created.

> DATABASE sysmaster;
Database closed.
Database selected.

> select name, is_logging, is_buff_log, is_ansi from sysdatabases where name='testdb';
name         testdb
is_logging   1
is_buff_log  0
is_ansi      0
1 row(s) retrieved.
>
```

Now, exit the docker container:

```text
$ onmode -ky
informix@77f7ee9ff52b:/$ exit
```

Then, follow the [Preparing Steps](https://www.ibm.com/docs/en/informix-servers/12.10?topic=api-preparing-use-change-data-capture) from Informix to create Informix CDC's system database and tables: 

```text
dbaccess  < $INFORMIXDIR/etc/syscdcv1.sql
```

### Finally, commit the image

```sh
$ docker commit ifx xiaolin/ifx12-cdc-test:v1
```

### Local Usage

```sh
$ docker run -it --name ifx --privileged -e SIZE=small \
    -p 9088:9088      \
    -p 9089:9089      \
    -p 27017:27017    \
    -p 27018:27018    \
    -p 27883:27883    \
    -e LICENSE=accept \
    xiaolin/ifx12-cdc-test:v1
```

### Using Docker Compose
> We can also run ifx container with `docker composer`.
> `docker-composer.yml` also integration with zookeeper & kafka for testing.

- start up informix(ifx) service only
```shell
$ cd [path-to-where-compose-file-is]
$ docker compose up ifx -d
or. 
$ docker compose -f <path-to-compose-file>/docker-compose.yml up ifx -d
e.g.
$ docker compose -f ~/cdc/docker-compose.yml up ifx -d
```
After the ifx container `STATUS` change to "running (healthy)". We can use informix database now.
```shell
 ⚡ root@localhost  ~/cdc  docker compose up ifx -d                                                                                                 ──(Mon,Jul04)─┘
[+] Running 2/2
 ⠿ Network cdc_default  Created                                                                                                                                0.2s
 ⠿ Container cdc-ifx-1  Started                                                                                                                                0.7s
 ⚡ root@localhost  ~/cdc  docker compose ps -a                                                                                                     ──(Mon,Jul04)─┘
NAME                COMMAND                  SERVICE             STATUS               PORTS
cdc-ifx-1           "/opt/ibm/dinit /opt…"   ifx                 running (starting)   0.0.0.0:9088-9089->9088-9089/tcp, 0.0.0.0:27017-27018->27017-27018/tcp, 0.0.0.0:27883->27883/tcp, :::9088-9089->9088-9089/tcp, :::27017-27018->27017-27018/tcp, :::27883->27883/tcp
 ⚡ root@localhost  ~/cdc  docker compose ps -a                                                                                                     ──(Mon,Jul04)─┘
NAME                COMMAND                  SERVICE             STATUS              PORTS
cdc-ifx-1           "/opt/ibm/dinit /opt…"   ifx                 running (healthy)   0.0.0.0:9088-9089->9088-9089/tcp, 0.0.0.0:27017-27018->27017-27018/tcp, 0.0.0.0:27883->27883/tcp, :::9088-9089->9088-9089/tcp, :::27017-27018->27017-27018/tcp, :::27883->27883/tcp
```

- start up all services within compose file(docker-compose.yml) 
```shell
$ docker compose up -d
```
- stop services with compose
```shell
$ docker compose down
```

## Reference

- https://hub.docker.com/r/ibmcom/informix-developer-database/tags
- https://github.com/informix/informix-server-dockerfiles
- https://stackoverflow.com/questions/61595224/undo-dockerfile-volume-directive-from-a-base-image
- https://github.com/informix/informix-dockerhub-readme
- https://www.ibm.com/docs/en/informix-servers/12.10?topic=api-preparing-use-change-data-capture
