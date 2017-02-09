import breeze.optimize.linear.LinearProgram;
import jdk.nashorn.internal.parser.JSONParser;
import netscape.javascript.JSException;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONException;
import org.json.JSONObject;

import javax.crypto.Mac;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;


/**
 * Created by Michael on 2/4/17.
 */
//TODO: A global primitive needed to track machines?
public class CamflowSystem {

    public static void main(String[] args) {//args: brokerURL Username Password
        //First connect to the broker and clear out all the old messages in the topics camflow/machines/#
        //TODO: Check the length of the arguments, so far the number of arguments is 3. This will change later.
        if (args.length < 3) {
            System.err.println("Usage: CamflowSystem <MQTTBrokerURL> <Username> <Password>");
            System.exit(1);
        }

        //Record the time so that packets before the boot of this system will be filtered
        Timestamp curnt = new Timestamp(System.currentTimeMillis());


        /* We use Spark to filter old messages now

        String topicsToClean = "camflow/machines/1022090165";
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setUserName(args[1]);
        options.setPassword(args[2].toCharArray());

        MemoryPersistence persistence = new MemoryPersistence();
        String clientId = MqttClient.generateClientId();
        try {
            MqttClient client = new MqttClient(args[0], clientId, persistence);
            //**********************************
            // Followings are to subscribe using MQTT API. This code will not be used; use Spark instead.

//            client.setCallback(new MqttCallback() {
//                @Override
//                public void connectionLost(Throwable throwable) {
//                    System.err.print("Connection lost.");
//                }
//
//                @Override
//                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
//                    System.out.println("Message topic:" + s);
//                    System.out.println("Message content: " + new String(mqttMessage.getPayload()));
//                }
//
//                @Override
//                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
//                    System.out.println("Delivery complete.");
//                }
//            });

            //*********************************
            client.connect(options);
            ClearMessages clearMsg = new ClearMessages();
            clearMsg.clearMessages(client, topicsToClean);
            //*********************************
            // Followings are to subscribe using MQTT API. This code will not be used; use Spark instead.
            // client.subscribe("camflow/machines/#");
            //*********************************
            client.disconnect();
            System.out.println("No old messages should be retained in the topics camflow/machines/ now.");
        } catch (MqttException me) {
            System.err.println("MQTT ClientConnectionForClearing Exception Reason Code:" + me.getReasonCode());
            System.err.println("MQTT ClientConnectionForClearing Exception Possible Cause:" + me.getCause());
        }

        */

        //At this point, use Spark for MQTT message streaming
        System.out.println("Initiating Spark for MQTT streaming");

        SparkSession spark = SparkSession
                .builder()
                .appName("CamFlowSystem")
                .master("local[4]")
                .getOrCreate();//TODO: Check API. What does it do?
        //Create DataStreamReader for streaming dataFrames
        Dataset<Row> packets = spark
                .readStream()
                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                .option("topic", "camflow/machines/#")
                .option("username", args[1])
                .option("password", args[2])
                .option("QoS", "2")
                .load(args[0]);
        Dataset<String> packetContent = packets
                .select("value") //SQL select operation on a data frame
                .as(Encoders.STRING()); //Common type (String) Encoder to serialize the objects so that a dataset can be created.

        //decode the packet content, serialize to JSON, and create a dataset of machine packet
        Dataset<MachinePacket> decodedPacketContent = packetContent.map(new MapFunction<String, MachinePacket>() {
            @Override
            public MachinePacket call(String str) throws IOException, DataFormatException, JSONException, java.text.ParseException {
                //decode packet using DecodePacket class
                DecodePacket dp = new DecodePacket();
                //Base 64 decode received packet
                byte[] decodedBytes = dp.decodeBase64(str);

                //Zlib decompress received packet
                byte[] output = dp.decodeZlib(decodedBytes);

                //serialize the string to JSON
                JSONObject machineJson = new JSONObject(new String(output));

                //Get machine ID and timestamp from JSONObject
                JsonHelper jh = new JsonHelper();
                List<String> machineInfo = jh.jsonMachinePacket(machineJson);

                //Create a machinePacket instance using machineInfo from Json
                MachinePacket mp = new MachinePacket();
                Iterator<String> itr = machineInfo.iterator();
                mp.setId(itr.next());
                Date format = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss").parse(itr.next());
                Timestamp ts = new Timestamp(format.getTime());
                mp.setTs(ts);
                return mp;
            }
        }, Encoders.bean(MachinePacket.class));

        //filter out old camflow/machines packets
        Dataset<String> filterMachinePacket = decodedPacketContent.filter(new FilterFunction<MachinePacket>() {
            @Override
            public boolean call(MachinePacket machinePacket) throws Exception {
                System.out.println("MachineTime:" + machinePacket.getTs());
                System.out.println("Startup Time:" + curnt);
                return (machinePacket.getTs().equals(curnt) || machinePacket.getTs().after(curnt));
            }
        })
                .select("id")
                .as(Encoders.STRING());

        StreamingQuery query = filterMachinePacket.writeStream()
                .foreach(new ForeachWriter<String>() {
                    @Override
                    public void process(String value) {
                        //create provenance process class to start process provenance
                        System.out.println("Start machine: " + value);
                        ProvenanceProcess pp = new ProvenanceProcess();
                        //Create DataStreamReader for streaming dataFrames
                        SparkSession sparkProv = SparkSession
                                .builder()
                                .appName("CamFlowSystemProvenance")
                                .master("local[4]")
                                .getOrCreate();//
                        Dataset<Row> provenance = sparkProv
                                .readStream()
                                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                                .option("topic", "camflow/provenance/" + value)
                                .option("username", args[1])
                                .option("password", args[2])
                                .option("QoS", "2")
                                .load(args[0]);
                        Dataset<ProvenancePacket> provInfo = pp.parseProvenance(provenance);


                        StreamingQuery provQuery = provInfo.writeStream()
                                .outputMode("append")
                                .format("console")
                                //.format("parquet")
                                //.option("path", "../out/output")
                                //.option("checkpointLocation", "../out/checkpoint")
                                .start();
                        try {
                            provQuery.awaitTermination();
                        } catch (StreamingQueryException sqe) {
                            System.err.println("The query terminated with an exception of the cause: " + sqe.cause());
                        }
                    }

                    @Override
                    public void close(Throwable errorOrNull) {

                    }

                    @Override
                    public boolean open(long partitionId, long version) {
                        return true;
                    }
                })
//                .outputMode("append")
//                .format("console")
                .start();

        try {
            query.awaitTermination();
        } catch(StreamingQueryException sqe) {
            System.err.println("The query terminated with an exception of the cause: " + sqe.cause());
        }

    }
}
