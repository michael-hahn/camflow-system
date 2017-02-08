import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    /**
     * This function extracts PROV-JSON info of node ID and node type into Pair (nodeId, nodeType)
     * @param jpp json object that contains packet about the provenance
     * @return a list of Pair (nodeId, nodeType)
     * @throws JSONException
     */
    public List<Pair<String, String>> jsonProvenanceNode (JSONObject jpp) throws JSONException {
        List<Pair<String, String>> retval = new ArrayList<Pair<String, String>>();
        Iterator<String> keys = jpp.keys();
        while (keys.hasNext()) {
            String key = keys.next(); //key can be "prefix", "entity", "activity", "used", "wasInformedBy", "wasGeneratedBy", "wasDerivedFrom"
            if (key.equals("activity") || key.equals("entity")) {
                if (jpp.get(key) instanceof JSONObject) {
                    JSONObject nodeInfo = (JSONObject) jpp.get(key);
                    Iterator<String> innerKeys = nodeInfo.keys();
                    while (innerKeys.hasNext()) {
                        String nodeId = innerKeys.next();
                        if (nodeInfo.get(nodeId) instanceof JSONObject) {
                            JSONObject info = (JSONObject) nodeInfo.get(nodeId);
                            Iterator<String> infoKeys = info.keys();
                            while (infoKeys.hasNext()) {
                                String infoTag = infoKeys.next();
                                if (infoTag.equals("prov:type")) {
                                    String nodeType = (String) info.get(infoTag);
                                    retval.add(Pair.of(nodeId, nodeType));
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
     * This function extracts PROV-JSON info of edges (identified by two nodes) and edge type
     * @param jpe json object that contains packet about the provenance
     * @return a list of Triples (fromNode, toNode, edgeType)
     * @throws JSONException
     */
    public List<Triple<String, String, String>> jsonProvenanceEdge (JSONObject jpe) throws JSONException {
        List<Triple<String, String, String>> retval = new ArrayList<Triple<String, String, String>>();
        Iterator<String> firstLevelTags = jpe.keys();
        while (firstLevelTags.hasNext()) {
            String firstLevelTag = firstLevelTags.next();
            if (firstLevelTag.equals("used") || firstLevelTag.equals("wasInformedBy")
                    || firstLevelTag.equals("wasDerivedFrom")) {
                if (jpe.get(firstLevelTag) instanceof JSONObject) {
                    JSONObject edgesInfo = (JSONObject) jpe.get(firstLevelTag);
                    Iterator<String> secondLevelTags = edgesInfo.keys();
                    while (secondLevelTags.hasNext()) {
                        String secondLevelTag = secondLevelTags.next();
                        if (edgesInfo.get(secondLevelTag) instanceof JSONObject) {
                            JSONObject edgeInfo = (JSONObject) edgesInfo.get(secondLevelTag);
                            Iterator<String> infoKeys = edgeInfo.keys();
                            while (infoKeys.hasNext()) {
                                String infoTag = infoKeys.next();
                                String edgeType = "";
                                String fromNode = "" ;
                                String toNode = "";
                                if (infoTag.equals("prov:type")) {
                                    edgeType = (String) edgeInfo.get(infoTag);
                                }
                                if (infoTag.equals("prov:entity") || infoTag.equals("prov:informant")
                                        || infoTag.equals("prov:usedEntity")) {
                                    fromNode = (String) edgeInfo.get(infoTag);
                                }
                                if (infoTag.equals("prov:activity") || infoTag.equals("prov:informed")
                                        || infoTag.equals("prov:generatedEntity")) {
                                    toNode = (String) edgeInfo.get(infoTag);
                                }
                                retval.add(Triple.of(fromNode, toNode, edgeType));
                            }
                        }
                    }
                }
            }
        }
        while (firstLevelTags.hasNext()) {
            String firstLevelTag = firstLevelTags.next();
            if (firstLevelTag.equals("wasGeneratedBy")) {
                if (jpe.get(firstLevelTag) instanceof JSONObject) {
                    JSONObject edgesInfo = (JSONObject) jpe.get(firstLevelTag);
                    Iterator<String> secondLevelTags = edgesInfo.keys();
                    while (secondLevelTags.hasNext()) {
                        String secondLevelTag = secondLevelTags.next();
                        if (edgesInfo.get(secondLevelTag) instanceof JSONObject) {
                            JSONObject edgeInfo = (JSONObject) edgesInfo.get(secondLevelTag);
                            Iterator<String> infoKeys = edgeInfo.keys();
                            while (infoKeys.hasNext()) {
                                String infoTag = infoKeys.next();
                                String edgeType = "";
                                String fromNode = "" ;
                                String toNode = "";
                                if (infoTag.equals("prov:type")) {
                                    edgeType = (String) edgeInfo.get(infoTag);
                                }
                                if (infoTag.equals("prov:activity")) {
                                    fromNode = (String) edgeInfo.get(infoTag);
                                }
                                if (infoTag.equals("prov:entity")) {
                                    toNode = (String) edgeInfo.get(infoTag);
                                }
                                retval.add(Triple.of(fromNode, toNode, edgeType));
                            }
                        }
                    }
                }
            }
        }
        return retval;
    }
}
