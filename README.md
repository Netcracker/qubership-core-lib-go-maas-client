[![Go build](https://github.com/Netcracker/qubership-core-lib-go-maas-client/actions/workflows/go-build.yml/badge.svg)](https://github.com/Netcracker/qubership-core-lib-go-maas-client/actions/workflows/go-build.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?metric=coverage&project=Netcracker_qubership-core-lib-go-maas-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-client)
[![duplicated_lines_density](https://sonarcloud.io/api/project_badges/measure?metric=duplicated_lines_density&project=Netcracker_qubership-core-lib-go-maas-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-client)
[![vulnerabilities](https://sonarcloud.io/api/project_badges/measure?metric=vulnerabilities&project=Netcracker_qubership-core-lib-go-maas-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-client)
[![bugs](https://sonarcloud.io/api/project_badges/measure?metric=bugs&project=Netcracker_qubership-core-lib-go-maas-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-client)
[![code_smells](https://sonarcloud.io/api/project_badges/measure?metric=code_smells&project=Netcracker_qubership-core-lib-go-maas-client)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-maas-client)

# MaaS go client
Go client to preform operations with MaaS

<!-- TOC -->
* [MaaS go client](#maas-go-client)
  * [Kafka](#kafka)
    * [MaaS Kafka client API](#maas-kafka-client-api)
    * [Get/Create/Delete topic from non default kafka instance](#getcreatedelete-topic-from-non-default-kafka-instance)
    * [Create MaaS Kafka go client with Cloud-Core default configuration](#create-maas-kafka-go-client-with-cloud-core-default-configuration)
    * [Use MaaS Kafka client with https://github.com/segmentio/kafka-go](#use-maas-kafka-client-with-httpsgithubcomsegmentiokafka-go)
    * [Implement messages consumption from Kafka topic in Blue-Green scenarios with https://github.com/segmentio/kafka-go](#implement-messages-consumption-from-kafka-topic-in-blue-green-scenarios-with-httpsgithubcomsegmentiokafka-go)
    * [Libraries dependency graph:](#libraries-dependency-graph)
  * [Rabbit](#rabbit)
    * [MaaS Rabbit client API](#maas-rabbit-client-api)
    * [Create MaaS Rabbit go client with Cloud-Core default configuration](#create-maas-rabbit-go-client-with-cloud-core-default-configuration)
  * [Retry behaviour](#retry-behaviour)
<!-- TOC -->

## Kafka
MaaS Kafka client to preform MaaS operations related to Kafka

### MaaS Kafka client API
[see API documentation](./kafka/api.go)

### Get/Create/Delete topic from non default kafka instance
Use 'Instance designators' feature in MaaS to control which kafka instance should be used. See this [documentation](https://github.com/netcracker/qubership-maas/blob/main/README.md#instance-designators)

### Create MaaS Kafka go client with Cloud-Core default configuration
To create MaaS kafka go client with Cloud-Core defaults use the following library extension
[libs/go/maas/core](https://github.com/netcracker/qubership-core-lib-go-maas-core)

### Use MaaS Kafka client with https://github.com/segmentio/kafka-go
To create pre-configured segmentio/kafka-go reader/writer/client structs via MaaS use the following extension
[libs/go/maas/segmentio](https://github.com/netcracker/qubership-core-lib-go-maas-segmentio)

### Implement messages consumption from Kafka topic in Blue-Green scenarios with https://github.com/segmentio/kafka-go
To create Blue-Green aware Kafka consumer use the following extension
[libs/go/maas/blue-green-segmentio](https://github.com/netcracker/qubership-core-lib-go-maas-bg-segmentio)

### Libraries dependency graph:
![maas-go-client.drawio.svg](maas-go-client.drawio.svg)


## Rabbit
MaaS Rabbit client to preform MaaS operations related to Rabbit

### MaaS Rabbit client API
[see API documentation](./rabbit/api.go)

### Create MaaS Rabbit go client with Cloud-Core default configuration
To create MaaS rabbit go client with Cloud-Core defaults use the following library extension
[libs/go/maas/core](https://github.com/netcracker/qubership-core-lib-go-maas-core)


## Retry behaviour

Every CRUD call to maas-agent (Kafka and Rabbit alike) is retried
`util.DefaultRetryAttempts` times with `util.DefaultRetryInterval` between
attempts. Retries stop early when the request context is done, so a caller
deadline always wins over the retry budget.

Which responses are retried:

| Response | Retried | Why |
|---|---|---|
| 5xx | yes | includes the `500` maas-agent returns when it cannot reach maas-service at all |
| 429 | yes | throttling |
| **405** | **yes** | maas-service maps PostgreSQL error `25006` (READ ONLY SQL TRANSACTION) to `405`, so a write against a demoted Patroni node during a leader switchover arrives as `405`, not as `5xx` |
| **401** | **yes** | the M2M token is re-fetched on every attempt, so an expired token or a briefly unavailable token provider clears itself on the next one |
| other 4xx | no | permanent client errors, failed on the first attempt |

The two 4xx entries are deliberate. Applying the usual "retry 5xx, fail fast on
4xx" rule here means not surviving a database leader switchover.

`GetTopic`/`GetVhost` keep treating `404` as "not found" and return `nil` after
a single request.
