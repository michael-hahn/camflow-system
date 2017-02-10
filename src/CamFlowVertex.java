import java.io.Serializable;
import java.util.List;

/**
 * Created by thomas on 09/02/17.
 */
public class CamFlowVertex implements Serializable {
    private String id;
    private String W3CType;
    private List<String[]> attributes;

    public String getId() {return this.id;}

    public String getW3CType() {return this.W3CType;}

    public List<String[]> getAttributes() {return this.attributes;}

    public void setId(String id) {this.id=id;}

    public void setW3CType(String type) {this.W3CType=type;}

    public void setAttributes(List<String[]> attributes) {this.attributes = attributes;}

}
