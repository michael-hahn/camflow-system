import java.io.Serializable;
import java.sql.Timestamp;
/**
 * Created by Michael on 2/6/17.
 */
public class MachinePacket implements Serializable {
    private String machineId;
    private Timestamp ts;

    public String getId () {
        return this.machineId;
    }

    public Timestamp getTs () {
        return this.ts;
    }

    public void setId (String machineId) {
        this.machineId = machineId;
    }

    public void setTs (Timestamp ts) {
        this.ts = ts;
    }
}

