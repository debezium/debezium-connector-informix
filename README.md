
[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Connector for Informix

This repository is an incubating Debezium Connector for [Informix Database](https://www.ibm.com/products/informix). We appreciated you can explore this connector and test it, and any feedback and contribution are welcome. This project aim for a community-ready Debezium Connector, which like many other official debezium connectors.

## Testing

### Debugging Tests

If you want to do local test, you can start a container manually:

```text
[xiaolin@172-20-3-242]$ docker run -it --name ifx --privileged -e SIZE=small -p 9088:9088 -p 9089:9089 -p 27017:27017 -p 27018:27018 -p 27883:27883 -e LICENSE=accept xiaolin/ifx12-cdc-test:v1
```

Then, you can run debugging from you IDE. And you can also run integration tests from command line
like this:

```text
$ mvn verify -Ddatabase.hostname=172.20.3.242
```

## Thanks

This codebase is derived from two projects: [debezium-connector-db2](https://github.com/debezium/debezium-connector-db2) and [debezium-informix-connector](https://github.com/laoflch/debezium-informix-connector). Thanks for originated ideas and Proof-of-Concept about how to integrate Informix CDC API with Debezium Connector Frameworks.
