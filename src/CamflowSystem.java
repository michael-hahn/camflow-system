import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;


/**
 * Created by Michael on 2/4/17.
 */
public class CamflowSystem {
    public static void main(String[] args) {//args: brokerURL Username Password
        //First connect to the broker and clear out all the old messages in the topics camflow/machines/#
        //TODO: Check the length of the arguments, so far the number of arguments is 3. This will change later.
        if (args.length < 3) {
            System.err.println("Usage: CamflowSystem <MQTTBrokerURL> <Username> <Password>");
            System.exit(1);
        }

        String topicsToClean = "camflow/machines";//TODO: Cannot clean subtopics of camflow/machines
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
            /*
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    System.err.print("Connection lost.");
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    System.out.println("Message topic:" + s);
                    System.out.println("Message content: " + new String(mqttMessage.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                    System.out.println("Delivery complete.");
                }
            });
            */
            //*********************************
            client.connect(options);
            ClearMessages clearMsg = new ClearMessages();
            clearMsg.clearMessages(client, topicsToClean);
            //*********************************
            // Followings are to subscribe using MQTT API. This code will not be used; use Spark instead.
            /*
            client.subscribe("camflow/machines/#");
            */
            //*********************************
            client.disconnect();
            System.out.println("No old messages should be retained in the topics camflow/machines/ now.");
        } catch (MqttException me) {
            System.err.println("MQTT ClientConnectionForClearing Exception Reason Code:" + me.getReasonCode());
            System.err.println("MQTT ClientConnectionForClearing Exception Possible Cause:" + me.getCause());
        }



        //At this point, use Spark for MQTT message streaming
        System.out.println("Initiating Spark for MQTT streaming");

        SparkSession spark = SparkSession
                .builder()
                .appName("CamFlowSystem")
                .master("local[4]")
                .getOrCreate();//TODO: Check API. What does it do?
        Dataset<Row> packets = spark
                .readStream()
                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                .option("topic", "camflow/machines/#")
                .option("username", args[1])
                .option("password", args[2])
                .option("QoS", "2")
                .load(args[0]);

        Dataset<String> packetContent = packets
                .select("value") //TODO: Check API. What else can I select besides "value"?
                .as(Encoders.STRING()); //TODO: IntelliJ does not seem to recognize this.
        Dataset<String> decodedPacketContent = packetContent.map(new MapFunction<String, String>() {
            @Override
            public String call (String str) throws IOException, DataFormatException {
                //Base 64 decode received packet
                byte[] decodedBytes = Base64.getDecoder().decode(str);
                //Zlib decompress received packet
                Inflater inflater = new Inflater();
                inflater.setInput(decodedBytes);

                ByteArrayOutputStream outputStream = new ByteArrayOutputStream(decodedBytes.length);
                byte[] buffer = new byte[1024];
                while (!inflater.finished()) {
                    int count = inflater.inflate(buffer);
                    outputStream.write(buffer, 0, count);
                }
                outputStream.close();
                byte[] output = outputStream.toByteArray();
                System.out.println("Packet Content:" + new String(output));
                return new String(output);
            }
        }, Encoders.STRING());

        Dataset<Row> counts = decodedPacketContent.groupBy("value").count();

        StreamingQuery query = counts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        } catch(StreamingQueryException sqe) {
            System.err.println("The query terminated with an exception of the cause: " + sqe.cause());
        }
    }
}
