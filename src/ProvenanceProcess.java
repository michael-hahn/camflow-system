import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Created by Michael on 2/7/17.
 */
public class ProvenanceProcess implements Serializable {
    public Dataset<ProvenancePacket> parseProvenance (Dataset<Row> provenance) {
        Dataset<String> provenanceContent = provenance
                .select("value") //SQL select operation on a data frame
                .as(Encoders.STRING());

        Dataset<ProvenancePacket> decodedProvenanceContent = provenanceContent.flatMap(new FlatMapFunction<String, ProvenancePacket>() {
            @Override
            public Iterator<ProvenancePacket> call(String s) throws IOException, DataFormatException, JSONException, java.text.ParseException {
                //decode the packet using DecodePacket class
                List<ProvenancePacket> retval = new ArrayList<ProvenancePacket>();
                DecodePacket dp = new DecodePacket();
                //Base 64 decode received packet
                byte[] decodedBytes = dp.decodeBase64(s);

                //Zlib decompress received packet
                byte[] output = dp.decodeZlib(decodedBytes);
                System.out.println("Packet Content:" + new String(output));

                //serialize the string to JSON
                JSONObject ProvenanceJson = new JSONObject(new String(output));

                //Get node and edge information from JSONObject
                JsonHelper jh = new JsonHelper();
                List<Pair<String, String>> nodeInfo = jh.jsonProvenanceNode(ProvenanceJson);
                List<Triple<String, String, String>> edgeInfo = jh.jsonProvenanceEdge(ProvenanceJson);

                //Create provenancePacket instances using the info from Json
                Iterator<Pair<String, String>> itr = nodeInfo.iterator();
                while (itr.hasNext()) {
                    Pair<String, String> node = itr.next();
                    ProvenancePacket ppnode = new ProvenancePacket();
                    ppnode.setFromNodeOrNode(node.getLeft());
                    ppnode.setToNodeOrZero("0");
                    ppnode.setType(node.getRight());
                    retval.add(ppnode);
                }
                Iterator<Triple<String, String, String>> itr2 = edgeInfo.iterator();
                while (itr2.hasNext()) {
                    Triple<String, String, String> edge = itr2.next();
                    ProvenancePacket ppedge = new ProvenancePacket();
                    ppedge.setFromNodeOrNode(edge.getLeft());
                    ppedge.setToNodeOrZero(edge.getMiddle());
                    ppedge.setType(edge.getRight());
                    retval.add(ppedge);
                }
                return retval.iterator();
            }
        }, Encoders.bean(ProvenancePacket.class));

        return decodedProvenanceContent;
    }
}
