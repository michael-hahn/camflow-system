import java.util.List;

/**
 * Created by thomas on 09/02/17.
 */
public class CamFlowEdge {
    private String srcId;
    private String dstId;
    private String W3CType;
    private List<String[]> attributes;

    public String getSrcId() {return this.srcId;}

    public String getDstId() {return this.dstId;}

    public String getW3CType() {return this.W3CType;}

    public List<String[]> getAttributes() {return this.attributes;}

    public void setSrcId(String id) {this.srcId=id;}

    public void setDstId(String id) {this.dstId=id;}

    public void setW3CType(String type) {this.W3CType=type;}

    public void setAttributes(List<String[]> attributes) {this.attributes = attributes;}
}
