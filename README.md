# CamFlow Intrusion Detection System
### Using provenance captured by CamFlow and Apache Spark DISC processing platform to detect system anomaly and intrusion
# Overview
### Processing Pipeline
#### Make CamFlowSystem Aware of The Existence of Machines To Monitor
- Machines joins by sending a machine packet to an MQTT broker at `camflow/machines` channel
  * This is managed by [CamFlow](https://github.com/CamFlow)
- CamFlowSystem listens at the same channel broadcast by the same MQTT broker
- Incoming machine packets are data sources processed by [Apache Spark Structured Streaming] (http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
  * Using MQTT as data sources to Spark is made possible by [Apache Bahir](http://bahir.apache.org)
- From the machines packets, CamFlowSystem knows what channel to listen to for the provenance packets of each machine
  * Machine packets contain machine ID, and each machine will send its provenance packets to `camflow/provenance/machineId`
  * CamFlowSystem creates a `SparkSession` for each channel and use Spark Structured Streaming to start processing packets from the channels
  * Again, Apache Bahir provides MQTT streaming source to the system

#### Interpret Provenance Packets
Currently, provenance packets are `Base64` encoded and `zlib` compressed. The contents are in the format of `PROV-JSON`
Interpreting provenance packets results in two `Dataset`s, one containing `CamFlowVertex` instances and one `CamFlowEdge` instances

- `Dataset<CamFlowVertex>`


| Property Name | Property Type (Java) | Description                          | Example                                      |
| :---          | :---                 | :---                                 | :---                                         |
| id            | `String`             | Unique ID that identifies the vertex | AQAAAAAAAEBWpAEAAAAAAP2S3vK12+s85QAAAAAAAAA= |
| W3CType       | `String`             | Type of the vertex/node              | task                                         |
| attributes    | `List<String[]>`     | All other attributes in PROV-JSON entity/activity | (cf:id, 107606) (cf:version, 229)            |


- `Dataset<CamFlowEdge>`


| Property Name | Property Type (Java) | Description                          | Example                                      |
| :---          | :---                 | :---                                 | :---                                         |
| srcId | `String` | The ID of the from node | AEAAAAAAACBjpAEAAAAAAP2S3vK12+s8AAAAAAAAAAA= |
| dstId | `String` | The ID of the to node | AQAAAAAAAEBWpAEAAAAAAP2S3vK12+s85QAAAAAAAAA= |
| W3CType | `String` | Type of the edge or the relation between two nodes | mmap_read, perm_read, mmap_write |
| attributes | `List<String[]>` | All other attributes in PROV-JSON used/wasInformedBy/wasGeneratedBy/wasDerivedFrom | (cf:id, 22922) (cf:offset, 832) |


# Example Usage
# Task List
- [x] Receive packets from MQTT broker
- [x] Interpret provenance packets and generating vertex and edge information
- [ ] Convert the vertex and edge datasets to VertexRDD and EdgeRDD
- [ ] Use better format than PROV-JSON
- [ ] Decide next steps

# Getting Started

First install required dependencies:
JDK 1.8, IntelliJ IDEA, git and maven

Then run the following commands:
```
git clone https://github.com/michael-hahn/camflow-system.git
cd camflow-system
make dependencies
```

Once the dependencies have finished building, open the project with IntelliJ.

