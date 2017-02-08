import java.io.Serializable;

/**
 * Created by Michael on 2/7/17.
 */
public class ProvenancePacket implements Serializable {
    private String type;
    private String fromNodeOrNode;
    private String toNodeOrZero; //0 iff it is a node packet, not an edge one

    public String getType () {
        return this.type;
    }

    public String getFromNodeOrNode () {
        return this.fromNodeOrNode;
    }

    public  String getToNodeOrZero () {
        return this.toNodeOrZero;
    }

    public void setType (String type) {
        this.type = type;
    }

    public void setFromNodeOrNode (String fromNodeOrNode) {
        this.fromNodeOrNode = fromNodeOrNode;
    }

    public void setToNodeOrZero (String toNodeOrZero) {
        this.toNodeOrZero = toNodeOrZero;
    }
}
