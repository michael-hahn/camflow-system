import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.javatuples.Quartet;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.zip.DataFormatException;

/**
 * Created by Michael on 2/6/17.
 */
public class JsonHelper {
    /**
     * @param jmp json object that contains packet about the machine
     * @return a list of string [machine_id, year:month:day hour:minute:second].
     * @throws JSONException
     */
    public List<String> jsonMachinePacket(JSONObject jmp) throws JSONException {
        List<String> retval = new ArrayList<String>();
        Iterator<String> keys = jmp.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            if (key.equals("entity")) {
                if (jmp.get(key) instanceof JSONObject) {
                    JSONObject machineKey = (JSONObject) jmp.get(key);
                    Iterator<String> innerKeys = machineKey.keys();
                    while (innerKeys.hasNext()) {
                        String id = innerKeys.next();
                        retval.add(id);
                        if (machineKey.get(id) instanceof JSONObject) {
                            JSONObject machineInfo = (JSONObject) machineKey.get(id);
                            Iterator<String> infoKeys = machineInfo.keys();
                            while (infoKeys.hasNext()) {
                                String info = infoKeys.next();
                                if (info.equals("cf:date")) {
                                    String date = (String) machineInfo.get(info);
                                    String[] elements = date.split("T");
                                    String time = elements[0] + " " + elements[1];
                                    retval.add(time);
                                }
                            }
                        }
                    }
                }
            }
        }
        return retval;
    }

    /**
     * This function takes a prov-json object and process its "entity" and "activity" key values.
     *
     * @param jpp json object that contains packet about the provenance
     * @return a list of node info list, which contains the node's type, id, boot id, machine id, and version if the node is not a packet
     * if the node is a packet, it contains the packet's type, id, snd ip, rcv ip, snd port, rcv port, protocol, and seq
     * @throws JSONException
     */
    public List<List<String>> jsonProvenanceNode(JSONObject jpp) throws JSONException, DataFormatException, IOException, org.apache.commons.codec.DecoderException {
        List<List<String>> retval = new ArrayList<List<String>>();
        DecodePacket dp = new DecodePacket();
        Iterator<String> keys = jpp.keys();
        while (keys.hasNext()) {
            String key = keys.next(); //key can be "prefix", "entity", "activity", "used", "wasInformedBy", "wasGeneratedBy", "wasDerivedFrom"
            if (key.equals("activity") || key.equals("entity")) {
                if (jpp.get(key) instanceof JSONObject) {
                    JSONObject nodeInfo = (JSONObject) jpp.get(key);
                    Iterator<String> innerKeys = nodeInfo.keys();
                    while (innerKeys.hasNext()) {
                        String nodeId = innerKeys.next();
                        byte[] decodedNodeId = dp.decodeBase64(nodeId);

                        //type can be packet or non-packet
                        //for packet: type (8 bytes) id (2 bytes) snd_ip (4 bytes) rcv_ip (4 bytes) snd_port (2 bytes) rcv_port (2 bytes) protocol (1 byte) seq (4 bytes)
                        //for non-packet node: type (8 bytes) id (8 bytes) boot_id (4 bytes) machine_id (4 bytes) version (4 bytes)
                        byte[] decodedType = Arrays.copyOfRange(decodedNodeId, 0, 8);
                        ArrayUtils.reverse(decodedType);

                        //check whether it is a packet or not
                        if (Hex.encodeHexString(decodedType).equals("2000000000100000")) {
                            retval.add(dp.decodePacket(decodedNodeId));
                        } else {
                            retval.add(dp.decodeNode(decodedNodeId));
                        }
                    }
                }
            }
        }
        return retval;
    }

    /**
     * This function extracts PROV-JSON info of edges (identified by two nodes) and edge type. It processes the "used"
     * "wasGeneratedBy", "wasInformedBy", and "wasDerivedFrom" key values
     *
     * @param jpe json object that contains packet about the provenance
     * @return a list of edge information list, which contains a triple of three list
     * the first list contains the from node's information
     * the second list contains the to node's information
     * the third list contains the information about the relationship of the two nods.
     * @throws JSONException
     */
    public List<Triple<List<String>, List<String>, List<String>>> jsonProvenanceEdge(JSONObject jpe) throws JSONException {
        List<Triple<List<String>, List<String>, List<String>>> retval = new ArrayList<Triple<List<String>, List<String>, List<String>>>();
        DecodePacket dp = new DecodePacket();
        Iterator<String> firstLevelTags = jpe.keys();
        while (firstLevelTags.hasNext()) {
            String firstLevelTag = firstLevelTags.next();
            if (firstLevelTag.equals("used") || firstLevelTag.equals("wasInformedBy")
                    || firstLevelTag.equals("wasDerivedFrom")) {
                if (jpe.get(firstLevelTag) instanceof JSONObject) {
                    JSONObject edgesInfo = (JSONObject) jpe.get(firstLevelTag);
                    Iterator<String> secondLevelTags = edgesInfo.keys();
                    while (secondLevelTags.hasNext()) {
                        String relationIdEncode = secondLevelTags.next();
                        byte[] decodedRelationId = dp.decodeBase64(relationIdEncode);
                        List<String> relation = dp.decodeRelation(decodedRelationId);
                        if (edgesInfo.get(relationIdEncode) instanceof JSONObject) {
                            List<String> fromNodeInfo = new ArrayList<String>();
                            List<String> toNodeInfo = new ArrayList<String>();
                            JSONObject edgeInfo = (JSONObject) edgesInfo.get(relationIdEncode);
                            Iterator<String> infoKeys = edgeInfo.keys();
                            while (infoKeys.hasNext()) {
                                String infoTag = infoKeys.next();
                                if (infoTag.equals("prov:entity") || infoTag.equals("prov:informant")
                                        || infoTag.equals("prov:usedEntity")) {
                                    String fromNode = (String) edgeInfo.get(infoTag);
                                    byte[] decodedNodeId = dp.decodeBase64(fromNode);
                                    byte[] decodedType = Arrays.copyOfRange(decodedNodeId, 0, 8);
                                    ArrayUtils.reverse(decodedType);
                                    if (Hex.encodeHexString(decodedType).equals("2000000000100000"))
                                        fromNodeInfo = dp.decodePacket(decodedNodeId);
                                    else fromNodeInfo = dp.decodeNode(decodedNodeId);
                                }
                                if (infoTag.equals("prov:activity") || infoTag.equals("prov:informed")
                                        || infoTag.equals("prov:generatedEntity")) {
                                    String toNode = (String) edgeInfo.get(infoTag);
                                    byte[] decodedNodeId = dp.decodeBase64(toNode);
                                    byte[] decodedType = Arrays.copyOfRange(decodedNodeId, 0, 8);
                                    ArrayUtils.reverse(decodedType);
                                    if (Hex.encodeHexString(decodedType).equals("2000000000100000"))
                                        toNodeInfo = dp.decodePacket(decodedNodeId);
                                    else toNodeInfo = dp.decodeNode(decodedNodeId);
                                }
                            }
                            if (!fromNodeInfo.isEmpty() && !toNodeInfo.isEmpty() && !relation.isEmpty())
                                retval.add(Triple.of(fromNodeInfo, toNodeInfo, relation));
                            else {
                                System.err.println("Bad-formatted JSON object -- Contact CamFlow");
                                System.exit(1);
                            }
                        }
                    }
                }
            }
            if (firstLevelTag.equals("wasGeneratedBy")) {
                if (jpe.get(firstLevelTag) instanceof JSONObject) {
                    JSONObject edgesInfo = (JSONObject) jpe.get(firstLevelTag);
                    Iterator<String> secondLevelTags = edgesInfo.keys();
                    while (secondLevelTags.hasNext()) {
                        String relationIdEncode = secondLevelTags.next();
                        byte[] decodedRelationId = dp.decodeBase64(relationIdEncode);
                        List<String> relation = dp.decodeRelation(decodedRelationId);
                        if (edgesInfo.get(relationIdEncode) instanceof JSONObject) {
                            List<String> fromNodeInfo = new ArrayList<String>();
                            List<String> toNodeInfo = new ArrayList<String>();
                            JSONObject edgeInfo = (JSONObject) edgesInfo.get(relationIdEncode);
                            Iterator<String> infoKeys = edgeInfo.keys();
                            while (infoKeys.hasNext()) {
                                String infoTag = infoKeys.next();
                                if (infoTag.equals("prov:activity")) {
                                    String fromNode = (String) edgeInfo.get(infoTag);
                                    byte[] decodedNodeId = dp.decodeBase64(fromNode);
                                    byte[] decodedType = Arrays.copyOfRange(decodedNodeId, 0, 8);
                                    ArrayUtils.reverse(decodedType);
                                    if (Hex.encodeHexString(decodedType).equals("2000000000100000"))
                                        fromNodeInfo = dp.decodePacket(decodedNodeId);
                                    else fromNodeInfo = dp.decodeNode(decodedNodeId);
                                }
                                if (infoTag.equals("prov:entity")) {
                                    String toNode = (String) edgeInfo.get(infoTag);
                                    byte[] decodedNodeId = dp.decodeBase64(toNode);
                                    byte[] decodedType = Arrays.copyOfRange(decodedNodeId, 0, 8);
                                    ArrayUtils.reverse(decodedType);
                                    if (Hex.encodeHexString(decodedType).equals("2000000000100000"))
                                        toNodeInfo = dp.decodePacket(decodedNodeId);
                                    else toNodeInfo = dp.decodeNode(decodedNodeId);
                                }
                            }
                            if (!fromNodeInfo.isEmpty() && !toNodeInfo.isEmpty() && !relation.isEmpty())
                                retval.add(Triple.of(fromNodeInfo, toNodeInfo, relation));
                            else {
                                System.err.println("Bad-formatted JSON object -- Contact CamFlow");
                                System.exit(1);
                            }
                        }
                    }
                }
            }
        }
        return retval;
    }

    /**
     * This function takes a JSONObject of one edge (not in wasGeneratedby) and returns a quartet of information
     * @param ep json object that contains the information of one edge
     * @return a quartet of information (from ProvJSON) of that edge: srcId, dstId, W3CType, attribute_list
     * @throws JSONException
     */
    public Quartet<String, String, String, List<String[]>> edgeProvenance(JSONObject ep) throws JSONException {
        Iterator<String> attrTags = ep.keys();
        String srcId = "";
        String dstId = "";
        String W3Ctype = "";
        List<String[]> attributes = new ArrayList<String[]>();
        while (attrTags.hasNext()) {
            String tag = attrTags.next();
            if (tag.equals("prov:type")) W3Ctype = (String) ep.get(tag);
            else if (tag.equals("prov:entity") || tag.equals("prov:informant") || tag.equals("prov:usedEntity"))
                srcId = (String) ep.get(tag);
            else if (tag.equals("prov:activity") || tag.equals("prov:informed") || tag.equals("prov:generatedEntity"))
                dstId = (String) ep.get(tag);
            else {
                String [] attrTuple = {tag, ep.get(tag).toString()};
                attributes.add(attrTuple);
            }
        }
        if (srcId.isEmpty() || dstId.isEmpty() || W3Ctype.isEmpty() || attributes.isEmpty()) {
            System.err.println("Missing essential edge information - Contact CamFlow");
            System.exit(1);
        }
        return Quartet.with(srcId, dstId, W3Ctype, attributes);
    }

    /**
     * This function takes a JSONObject of one edge (wasGeneratedby only) and return a quartet of information
     *
     * @param ep json object that contains the information of one edge
     * @return a quartet of information (ProvJSON) of that edge: srcId, dstId, W3CType, attribute_list
     * @throws JSONException
     */
    public Quartet<String, String, String, List<String[]>> edgeProvenanceWGB (JSONObject ep) throws JSONException {
        Iterator<String> attrTags = ep.keys();
        String srcId = "";
        String dstId = "";
        String W3Ctype = "";
        List<String[]> attributes = new ArrayList<String[]>();
        while (attrTags.hasNext()) {
            String tag = attrTags.next();
            if (tag.equals("prov:type")) W3Ctype = (String) ep.get(tag);
            else if (tag.equals("prov:activity"))
                srcId = (String) ep.get(tag);
            else if (tag.equals("prov:entity"))
                dstId = (String) ep.get(tag);
            else {
                String [] attrTuple = {tag, ep.get(tag).toString()};
                attributes.add(attrTuple);
            }
        }
        if (srcId.isEmpty() || dstId.isEmpty() || W3Ctype.isEmpty() || attributes.isEmpty()) {
            System.err.println("Missing essential edge information in WGB - Contact CamFlow");
            System.exit(1);
        }
        return Quartet.with(srcId, dstId, W3Ctype, attributes);
    }

    /**
     * This function takes a JSONObject of one vertex (from both entity and activity) and returns a pair of information
     * @param vp json object that contains the information of one vertex
     * @return a pair of information (from ProvJSON) of that vertex: W3CType, attribute_list
     * @throws JSONException
     */
    public Pair<String, List<String[]>> vertexProvenance (JSONObject vp) throws JSONException {
        Iterator<String> attrTags = vp.keys();
        String W3CType = "";
        List<String[]> attributes = new ArrayList<String[]>();
        while (attrTags.hasNext()) {
            String tag = attrTags.next();
            if (tag.equals("prov:type")) W3CType = (String) vp.get(tag);
            else {
                String [] attrTuple = {tag, vp.get(tag).toString()};
                attributes.add(attrTuple);
            }
        }
        if (W3CType.isEmpty() || attributes.isEmpty()) {
            System.err.println("Missing essential edge information - Contact CamFlow");
            System.exit(1);
        }
        return Pair.of(W3CType, attributes);
    }

    /**
     * This function takes provenance info of all edges in a packet and returns a list of results
     * from both edgeProvenance and edgeProvenanceWGB
     * @param prov provenance packet in json format received from CamFlow
     * @return a list of results from both edgeProvenance and edgeProvenanceWGB
     * @throws JSONException
     */
    public List<Quartet<String, String, String, List<String[]>>> edgesProvenance (JSONObject prov) throws JSONException {
        Iterator<String> tags = prov.keys();
        List<Quartet<String, String, String, List<String[]>>> retval = new ArrayList<Quartet<String, String, String, List<String[]>>>();
        while (tags.hasNext()) {
            String tag = tags.next();
            if (tag.equals("used") || tag.equals("wasGeneratedBy") || tag.equals("wasInformedBy") || tag.equals("wasDerivedFrom")) {
                if (prov.get(tag) instanceof JSONObject) {
                    JSONObject edges = (JSONObject) prov.get(tag);
                    Iterator<String> edgeTags = edges.keys();
                    while (edgeTags.hasNext()) {
                        String edgeTag = edgeTags.next();
                        if (edges.get(edgeTag) instanceof JSONObject) {
                            JSONObject edge = (JSONObject) edges.get(edgeTag);
                            if (tag.equals("wasGeneratedBy")) {
                                Quartet<String, String, String, List<String[]>> edgeInfo = edgeProvenanceWGB(edge);
                                retval.add(edgeInfo);
                            } else {
                                Quartet<String, String, String, List<String[]>> edgeInfo = edgeProvenance(edge);
                                retval.add(edgeInfo);
                            }
                        }
                    }
                }
            }
        }
        return retval;
    }

    /**
     * This function takes provenance packet in json format and return the information of all vertices in the packet
     * @param prov provenance packet in json format received from CamFlow
     * @return a list of pairs in the form: vertexId, results from vertexProvenance
     * @throws JSONException
     */
    public List<Pair<String, Pair<String, List<String[]>>>> verticesProvenance (JSONObject prov) throws JSONException {
        Iterator<String> tags = prov.keys();
        List<Pair<String, Pair<String, List<String[]>>>> retval = new ArrayList<Pair<String, Pair<String, List<String[]>>>>();
        while (tags.hasNext()) {
            String tag = tags.next();
            if (tag.equals("entity") || tag.equals("activity")) {
                if (prov.get(tag) instanceof JSONObject) {
                    JSONObject vertices = (JSONObject) prov.get(tag);
                    Iterator<String> vertexTags = vertices.keys();
                    while (vertexTags.hasNext()) {
                        String vertexTag = vertexTags.next();
                        if (vertices.get(vertexTag) instanceof JSONObject) {
                            JSONObject vertex = (JSONObject) vertices.get(vertexTag);
                            Pair<String, List<String[]>> vertexInfo = vertexProvenance(vertex);
                            retval.add(Pair.of(vertexTag, vertexInfo));
                        }
                    }
                }
            }
        }
        return retval;
    }

}