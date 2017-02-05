import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Created by Michael on 2/4/17.
 */
public class ClearMessages {
    public void clearMessages (MqttClient client, String topic) {
        try {
            String content = "";
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setRetained(true);
            client.publish(topic, message);
            System.out.println("Clear old messages in topic: " + topic);
        } catch (MqttException me) {
            System.err.println("MQTT ClearMessages Exception Reason Code:" + me.getReasonCode());
            System.err.println("MQTT ClearMessages Exception Possible Cause:" + me.getCause());
        }
    }
}
