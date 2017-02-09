import io.netty.handler.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.json.JSONException;
import org.json.JSONObject;
import scala.math.Ordering;

import java.io.IOException;
import java.math.BigInteger;
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
    public List<String> jsonMachinePacket (JSONObject jmp) throws JSONException {
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

    /**TODO: Fix this documentation
     *
     * @param jpp json object that contains packet about the provenance
     * @return
     * @throws JSONException
     */
    public List<List<String>> jsonProvenanceNode (JSONObject jpp) throws JSONException, DataFormatException, IOException, org.apache.commons.codec.DecoderException {
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

    /** TODO: Fix this documentation
     * This function extracts PROV-JSON info of edges (identified by two nodes) and edge type
     * @param jpe json object that contains packet about the provenance
     * @return
     * @throws JSONException
     */
    public List<Triple<List<String>, List<String>, List<String>>> jsonProvenanceEdge (JSONObject jpe) throws JSONException {
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
}
