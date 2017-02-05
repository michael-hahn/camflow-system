# CamFlow Intrusion Detection System
## Using provenance captured by CamFlow and Apache Spark DISC processing platform to detect system anomaly and intrusion
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
3. In Project Structure again, Choose From Maven...
Import org.apache.bahir:assembly_2.11:2.0.1
This is for supporting MQTT with Spark
4. **Warning: bahir contains a dependency issue**
   * You can solve this issue by importing the following package
     `org.eclipse.paho.client.mqtt3-1.1.0`
5. If you performed step 4 (aka import mqtt after bahir), go to
`camflow-system.iml`
Swap the order of the entries (tagged <orderEntry...>) with name tag "...mqttv3-1.1.0" and "...bahir:assembly_2.11:2.0.1". 
