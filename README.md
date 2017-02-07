# CamFlow Intrusion Detection System
### Using provenance captured by CamFlow and Apache Spark DISC processing platform to detect system anomaly and intrusion
# Overview
# Example Usage
# Getting Started
## Following instructions are for developing in IntelliJIDEA
1. To set up IntelliJ environment. Download Spark source code (not the ones with Hadoop, but do download the one with all extensions including Spark SQL, Structured Streaming, and GraphX), extract and build with the following command:
`build/mvn -DskipTests clean package`
2. Once built. In the Project Structure -> Libraries
Click "+"
Choose Java
Import 
`Spark 2.1.0/assembly/target/scala-2.11/jars`
3. Download latest Apache Bahir online. Currently I am using Bahir 2.0.2.
4. Build Bahir using the following command after extraction
`mvn -DskipTests clean install`
This should be built using Maven 3.3.9 or above.
5. In Project Structure again -> Libraries -> + -> Choose Java
Import
`apache-bahir-2.0.2-src/sql-streaming-mqtt/target/spark-sql-streaming-mqtt_2.11-2.0.2.jar`
and
`spark-streaming-mqtt_2.11-2.0.2.jar`
This is for supporting MQTT with Spark
4. **Warning: bahir contains a dependency issue**
   * You can solve this issue by importing the following package
     `org.eclipse.paho.client.mqtt3-1.1.0`
5. Step 4 may not be necessary.
