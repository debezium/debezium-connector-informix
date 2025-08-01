
[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://img.shields.io/maven-central/v/io.debezium/debezium-connector-informix)](https://search.maven.org/#search|ga|1|g:io.debezium+a:debezium-connector-informix)
[![Build Status](https://github.com/debezium/debezium-connector-informix/actions/workflows/maven.yml/badge.svg)](https://github.com/debezium/debezium-connector-informix/actions/workflows/maven.yml)
[![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)](https://gitter.im/debezium/user)
[![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)](https://gitter.im/debezium/dev)
[![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)](https://groups.google.com/forum/#!forum/debezium)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)](http://stackoverflow.com/questions/tagged/debezium)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Connector for Informix

Debezium is an open source project that provides a low latency data streaming platform for change data capture (CDC).

This repository contains the connector for IBM Informix.
You are encouraged to explore this connector and test it. Although specific features may not be implemented yet, it should be stable enough for production usage.

Documentation on how to use the connector and the internal workings can be found [here](https://debezium.io/documentation/reference/stable/connectors/informix.html). See in this [Dockerfile](src/test/docker/informix-cdc-docker/14/Dockerfile) how [this script](src/test/docker/informix-cdc-docker/14/informix_post_init.sh) is used to set up the CDC database in the docker Informix instance.

## Building and testing the Informix connector

Building this connector first requires the main [debezium](https://github.com/debezium/debezium) code repository to be built locally using `mvn clean install`.

Then after, run Informix using `mvn install` will compile all code and run the unit and integration tests. If there are any compile problems or any of the unit tests fail, the build will stop immediately. Otherwise, the command will continue to create the module's artifacts, create the Docker image with Informix and custom scripts, start the Docker container, run the integration tests, stop the container (even if there are integration test failures), and run checkstyle on the code. If there are still no problems, the build will then install the module's artifacts into the local Maven repository.

An *integration test* is a JUnit test class named `*IT.java` or `IT*.java` that uses a Informix database server running in a custom Docker container based upon the [ibmcom/informix-developer-database](https://hub.docker.com/r/ibmcom/informix-developer-database) Docker image maintained by the Informix team. The build will automatically start the Informix container before the integration tests are run and automatically stop and remove it after all of the integration tests complete (regardless of whether they succeed or fail). All databases used in the integration tests are defined and populated using `*.sql` files and `*.sh` scripts in the `src/test/docker/informix-cdc-docker` directory, which are copied into the Docker image and run by Informix upon startup. Multiple test methods within a single integration test class can reuse the same database, but generally each integration test class should use its own dedicated database(s).

You should always default to using `mvn install`, especially prior to committing changes to Git. However, there are a few situations where you may want to run a different Maven command. For details on running individual tests or inspecting the Informix database for debugging continue reading below.

## Using the Informix connector with Kafka Connect

The Informix connector is designed to work with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) and to be deployed to a Kafka Connect runtime service. The deployed connector will monitor one or more databases and write all change events to Kafka topics, which can be independently consumed by one or more clients. Kafka Connect can be distributed to provide fault tolerance to ensure the connectors are running and continually keeping up with changes in the database.

Kafka Connect can also be run standalone as a single process, although doing so is not tolerant of failures.

## Embedding the Informix connector

The Informix connector can also be used as a library without Kafka or Kafka Connect, enabling applications and services to directly connect to a Informix database and obtain the ordered change events. This approach requires the application to record the progress of the connector so that upon restart the connect can continue where it left off. Therefore, this may be a useful approach for less critical use cases. For production use cases, we highly recommend using this connector with Kafka and Kafka Connect.

## Testing

This module contains both unit tests and integration tests.

A *unit test* is a JUnit test class named `*Test.java` or `Test*.java` that never requires or uses external services, though it can use the file system and can run any components within the same JVM process. They should run very quickly, be independent of each other, and clean up after itself.

An *integration test* is a JUnit test class named `*IT.java` or `IT*.java` that uses a Informix database server running in a custom Docker container based upon the [ibmcom/informix-developer-database](https://hub.docker.com/r/ibmcom/informix-developer-database) Docker image maintained by the Informix team. The build will automatically start the Informix container before the integration tests are run and automatically stop and remove it after all of the integration tests complete (regardless of whether they suceed or fail). All databases used in the integration tests are defined and populated using `*.sql` files and `*.sh` scripts in the `src/test/docker/informix-cdc-docker` directory, which are copied into the Docker image and run by Informix upon startup. Multiple test methods within a single integration test class can reuse the same database, but generally each integration test class should use its own dedicated database(s).

Running `mvn install` will compile all code and run the unit and integration tests. If there are any compile problems or any of the unit tests fail, the build will stop immediately. Otherwise, the command will continue to create the module's artifacts, create the Docker image with Informix and custom scripts, start the Docker container, run the integration tests, stop the container (even if there are integration test failures), and run checkstyle on the code. If there are still no problems, the build will then install the module's artifacts into the local Maven repository.

You should always default to using `mvn install`, especially prior to committing changes to Git. However, there are a few situations where you may want to run a different Maven command.

### Running some tests

If you are trying to get the test methods in a single integration test class to pass and would rather not run *all* of the integration tests, you can instruct Maven to just run that one integration test class and to skip all of the others. For example, use the following command to run the tests in the `ConnectionIT.java` class:

    $ mvn -Dit.test=ConnectionIT install

Of course, wildcards also work:

    $ mvn -Dit.test=Connect*IT install

These commands will automatically manage the Informix Docker container.

### Debugging tests

If you want to debug integration tests by stepping through them in your IDE, using the `mvn install` command will be problematic since it will not wait for your IDE's breakpoints. There are ways of doing this, but it is typically far easier to simply start the Docker container and leave it running so that it is available when you run the integration test(s). The following command:

    $ mvn docker:start

will start the default Informix container and run the database server. Now you can use your IDE to run/debug one or more integration tests. Just be sure that the integration tests clean up their database before (and after) each test, and that you run the tests with VM arguments that define the required system properties, including:

* `database.dbname` - the name of the database that your integration test will use; there is no default
* `database.hostname` - the IP address or name of the host where the Docker container is running; defaults to `localhost` which is likely for Linux, but on OS X and Windows Docker it will have to be set to the IP address of the VM that runs Docker (which you can find by looking at the `DOCKER_HOST` environment variable).
* `database.port` - the port on which Informix is listening; defaults to `9088` and is what this module's Docker container uses
* `database.user` - the name of the database user; defaults to `informix` and is correct unless your database script uses something different
* `database.password` - the password of the database user; defaults to `in4mix` and is correct unless your database script uses something different

For example, you can define these properties by passing these arguments to the JVM:

    -Ddatabase.dbname=<DATABASE_NAME> -Ddatabase.hostname=<DOCKER_HOST> -Ddatabase.port=9088 -Ddatabase.user=informix -Ddatabase.password=in4mix

When you are finished running the integration tests from your IDE, you have to stop and remove the Docker container before you can run the next build:

    $ mvn docker:stop


Please note that when running the Informix database Docker container, the output is written to the Maven build output and includes several lines with `[Warning] Using a password on the command line interface can be insecure.` You can ignore these warnings, since we don't need a secure database server for our transient database testing.

### Analyzing the database

Sometimes you may want to inspect the state of the database(s) after one or more integration tests are run. The `mvn install` command runs the tests but shuts down and removes the container after the integration tests complete. To keep the container running after the integration tests complete, use this Maven command:

    $ mvn integration-test

### Stopping the Docker container

This instructs Maven to run the normal Maven lifecycle through `integration-test`, and to stop before the `post-integration-test` phase when the Docker container is normally shut down and removed. Be aware that you will need to manually stop and remove the container before running the build again:

    $ mvn docker:stop

### Testing all Informix configurations

In Debezium builds, the `assembly` profile is used when issuing a release or in our continuous integration builds. In addition to the normal steps, it also creates several additional artifacts (including the connector plugin's ZIP and TAR archives) and runs the whole
integration test suite once for _each_ of the Informix configurations. If you want to make sure that your changes work on all Informix configurations, add `-Passembly` to your Maven commands.


```
name=informix-connector
connector.class=io.debezium.connector.informix.InformixConnector
database.hostname=localhost
database.port=9088
database.user=informix
database.password=in4mix
database.dbname=testdb
database.history.kafka.bootstrap.servers=localhost:9092
database.history.kafka.topic=cdctestdb
```

### Building just the artifacts, without running tests, CheckStyle, etc.

You can skip all non-essential plug-ins (tests, integration tests, CheckStyle, formatter, API compatibility check, etc.) using the "quick" build profile:

    $ mvn clean verify -Dquick

This provides the fastes way for solely producing the output artifacts, without running any of the QA related Maven plug-ins.
This comes in handy for producing connector JARs and/or archives as quickly as possible, e.g. for manual testing in Kafka Connect.
