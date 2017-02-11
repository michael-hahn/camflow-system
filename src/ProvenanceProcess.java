import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.javatuples.Quartet;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.zip.DataFormatException;

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
            public Iterator<ProvenancePacket> call(String s) throws IOException, DataFormatException, JSONException, java.text.ParseException, org.apache.commons.codec.DecoderException {
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
                //List<List<String>> nodeInfo = jh.jsonProvenanceNode(ProvenanceJson); //TODO: Not needed for the system. Testing only.
                List<Triple<List<String>, List<String>, List<String>>> edgeInfo = jh.jsonProvenanceEdge(ProvenanceJson);

                /*
                //Create provenancePacket instances using the info from Json
                //TODO: This is node information. Not needed in the system. Use Edge info instead. For testing only.
                Iterator<List<String>> itr = nodeInfo.iterator();
                while (itr.hasNext()) {
                    ProvenancePacket ppnode = new ProvenancePacket();
                    List<String> node = itr.next();
                    Iterator<String> nodeItr = node.iterator();
                    String type = nodeItr.next();
                    if (type.equals("2000000000100000")) {
                        String id = nodeItr.next();
                        String sndIp = nodeItr.next();
                        String rcvIp = nodeItr.next();
                        String sndPort = nodeItr.next();
                        String rcvPort = nodeItr.next();
                        String protocol = nodeItr.next();
                        String seq = nodeItr.next();
                        ppnode.setFromNodeType(type);
                        ppnode.setFromNodeId(id);
                        ppnode.setFromNodeSndIp(sndIp);
                        ppnode.setFromNodeRcvIp(rcvIp);
                        ppnode.setFromNodeSndPort(sndPort);
                        ppnode.setFromNodeRcvPort(rcvPort);
                        ppnode.setFromNodeProtocol(protocol);
                        ppnode.setFromNodeSeq(seq);
                    } else {
                        String id = nodeItr.next();
                        String bootId = nodeItr.next();
                        String machineId = nodeItr.next();
                        String version = nodeItr.next();
                        ppnode.setFromNodeType(type);
                        ppnode.setFromNodeId(id);
                        ppnode.setFromNodeBootId(bootId);
                        ppnode.setFromNodeMachineId(machineId);
                        ppnode.setFromNodeVersion(version);
                    }
                    retval.add(ppnode);
                }
                */

                Iterator<Triple<List<String>, List<String>, List<String>>> itr = edgeInfo.iterator();
                while (itr.hasNext()) {
                    Triple<List<String>, List<String>, List<String>> edge = itr.next();
                    List<String> fromNodeInfo = edge.getLeft();
                    List<String> toNodeInfo = edge.getMiddle();
                    List<String> relation = edge.getRight();

                    ProvenancePacket ppedge = new ProvenancePacket();

                    Iterator<String> fromNodeItr = fromNodeInfo.iterator();
                    String fromType = fromNodeItr.next();
                    if (fromType.equals("2000000000100000")) {
                        String id = fromNodeItr.next();
                        String sndIp = fromNodeItr.next();
                        String rcvIp = fromNodeItr.next();
                        String sndPort = fromNodeItr.next();
                        String rcvPort = fromNodeItr.next();
                        String protocol = fromNodeItr.next();
                        String seq = fromNodeItr.next();
                        ppedge.setFromNodeType(fromType);
                        ppedge.setFromNodeId(id);
                        ppedge.setFromNodeSndIp(sndIp);
                        ppedge.setFromNodeRcvIp(rcvIp);
                        ppedge.setFromNodeSndPort(sndPort);
                        ppedge.setFromNodeRcvPort(rcvPort);
                        ppedge.setFromNodeProtocol(protocol);
                        ppedge.setFromNodeSeq(seq);
                    } else {
                        String id = fromNodeItr.next();
                        String bootId = fromNodeItr.next();
                        String machineId = fromNodeItr.next();
                        String version = fromNodeItr.next();
                        ppedge.setFromNodeType(fromType);
                        ppedge.setFromNodeId(id);
                        ppedge.setFromNodeBootId(bootId);
                        ppedge.setFromNodeMachineId(machineId);
                        ppedge.setFromNodeVersion(version);
                    }

                    Iterator<String> toNodeItr = toNodeInfo.iterator();
                    String toType = toNodeItr.next();
                    if (toType.equals("2000000000100000")) {
                        String id = toNodeItr.next();
                        String sndIp = toNodeItr.next();
                        String rcvIp = toNodeItr.next();
                        String sndPort = toNodeItr.next();
                        String rcvPort = toNodeItr.next();
                        String protocol = toNodeItr.next();
                        String seq = toNodeItr.next();
                        ppedge.setToNodeType(toType);
                        ppedge.setToNodeId(id);
                        ppedge.setToNodeSndIp(sndIp);
                        ppedge.setToNodeRcvIp(rcvIp);
                        ppedge.setToNodeSndPort(sndPort);
                        ppedge.setToNodeRcvPort(rcvPort);
                        ppedge.setToNodeProtocol(protocol);
                        ppedge.setToNodeSeq(seq);
                    } else {
                        String id = toNodeItr.next();
                        String bootId = toNodeItr.next();
                        String machineId = toNodeItr.next();
                        String version = toNodeItr.next();
                        ppedge.setToNodeType(toType);
                        ppedge.setToNodeId(id);
                        ppedge.setToNodeBootId(bootId);
                        ppedge.setToNodeMachineId(machineId);
                        ppedge.setToNodeVersion(version);
                    }

                    Iterator<String> relationItr = relation.iterator();
                    String relationType = relationItr.next();
                    String relationId = relationItr.next();
                    String relationBootId = relationItr.next();
                    String relationMachineId = relationItr.next();
                    ppedge.setRelationType(relationType);
                    ppedge.setRelationId(relationId);
                    ppedge.setRelationBootId(relationBootId);
                    ppedge.setRelationMachineID(relationMachineId);

                    retval.add(ppedge);
                }
                return retval.iterator();
            }
        }, Encoders.bean(ProvenancePacket.class));

        return decodedProvenanceContent;
    }


    /**
     * This function creates a dataset of CamFlowEdge information
     * @param provenance provenance packet from CamFlow
     * @return a dataset of type CamFlowEdge
     */
    public Dataset<CamFlowEdge> edges(Dataset<Row> provenance){
        Dataset<CamFlowEdge> edgeContent = provenance
                .select("value")
                .as(Encoders.STRING())
                .flatMap(new FlatMapFunction<String, CamFlowEdge>() {
                    @Override
                    public Iterator<CamFlowEdge> call(String s) throws Exception {
                        //return value of this call function initialized
                        List<CamFlowEdge> retval = new ArrayList<CamFlowEdge>();

                        //decodePacket class for decoding the packet
                        DecodePacket dp = new DecodePacket();
                        //Base 64 decode received packet
                        byte[] decodedBytes = dp.decodeBase64(s);
                        //Zlib decompress received packet
                        byte[] output = dp.decodeZlib(decodedBytes);
                        //for debug only
                        System.out.println("Packet Content:" + new String(output));

                        //serialize the string to JSON
                        JSONObject provenanceJson = new JSONObject(new String(output));

                        //get edge information from JSONObject
                        JsonHelper jh = new JsonHelper();
                        List<Quartet<String, String, String, List<String[]>>> edgesInfo = jh.edgesProvenance(provenanceJson);

                        //generate CamFlowEdge instances of each element in edgesInfo list
                        Iterator<Quartet<String, String, String, List<String[]>>> itr = edgesInfo.iterator();
                        while (itr.hasNext()) {
                            Quartet<String, String, String, List<String[]>> edgeInfo = itr.next();
                            CamFlowEdge cfe = new CamFlowEdge();
                            cfe.setSrcId(edgeInfo.getValue0());
                            cfe.setDstId(edgeInfo.getValue1());
                            cfe.setW3CType(edgeInfo.getValue2());
                            cfe.setAttributes(edgeInfo.getValue3());
                            retval.add(cfe);
                        }
                        return retval.iterator();
                    }
                }, Encoders.bean(CamFlowEdge.class));
        return edgeContent;
    }

    /**
     * This function creates a dataset of CamFlowVertex information
     * @param provenance provenance packet from CamFlow
     * @return a dataset of type CamFlowVertex
     */
    public Dataset<CamFlowVertex> vertices(Dataset<Row> provenance) {
        Dataset<CamFlowVertex> vertexContent = provenance
                .select("value")
                .as(Encoders.STRING())
                .flatMap(new FlatMapFunction<String, CamFlowVertex>() {
                    @Override
                    public Iterator<CamFlowVertex> call(String s) throws Exception {
                        //return value of this call function initialized
                        List<CamFlowVertex> retval = new ArrayList<CamFlowVertex>();

                        //decodePacket class for decoding the packet
                        DecodePacket dp = new DecodePacket();
                        //Base 64 decode received packet
                        byte[] decodedBytes = dp.decodeBase64(s);
                        //Zlib decompress received packet
                        byte[] output = dp.decodeZlib(decodedBytes);
                        //for debug only
                        System.out.println("Packet Content:" + new String(output));

                        //serialize the string to JSON
                        JSONObject provenanceJson = new JSONObject(new String(output));

                        //get vertice information from JSONObject
                        JsonHelper jh = new JsonHelper();
                        List<Pair<String, Pair<String, List<String[]>>>> verticesInfo = jh.verticesProvenance(provenanceJson);

                        //generate CamFlowEdge instances of each element in verticesInfo list
                        Iterator<Pair<String, Pair<String, List<String[]>>>> itr = verticesInfo.iterator();
                        while (itr.hasNext()) {
                            Pair<String, Pair<String, List<String[]>>> vertexInfo = itr.next();
                            CamFlowVertex cfv = new CamFlowVertex();
                            cfv.setId(vertexInfo.getLeft());
                            cfv.setW3CType(vertexInfo.getRight().getLeft());
                            cfv.setAttributes(vertexInfo.getRight().getRight());
                            retval.add(cfv);
                        }
                        return retval.iterator();
                    }
                }, Encoders.bean(CamFlowVertex.class));
        return vertexContent;
    }
}
