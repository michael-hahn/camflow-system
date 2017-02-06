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
}
