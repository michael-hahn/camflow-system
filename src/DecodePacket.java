import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Created by Michael on 2/7/17.
 */
public class DecodePacket {
    public byte[] decodeBase64 (String str) {
        return Base64.getDecoder().decode(str);
    }

    public byte[] decodeZlib (byte[] bytes) throws DataFormatException, IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(bytes);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bytes.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        return outputStream.toByteArray();
    }

    public List<String> decodePacket (byte[] decodedNodeId) {
        byte[] decodedType = Arrays.copyOfRange(decodedNodeId, 0, 8);
        ArrayUtils.reverse(decodedType);
        byte[] decodedId = Arrays.copyOfRange(decodedNodeId, 8, 10);
        ArrayUtils.reverse(decodedId);
        byte[] decodedSndIp = Arrays.copyOfRange(decodedNodeId, 10, 14);
        ArrayUtils.reverse(decodedSndIp);
        byte[] decodedRcvIp = Arrays.copyOfRange(decodedNodeId, 14, 18);
        ArrayUtils.reverse(decodedRcvIp);
        byte[] decodedSndPort = Arrays.copyOfRange(decodedNodeId, 18, 20);
        ArrayUtils.reverse(decodedSndPort);
        byte[] decodedRcvPort = Arrays.copyOfRange(decodedNodeId, 20, 22);
        ArrayUtils.reverse(decodedRcvPort);
        byte[] decodedProtocol = Arrays.copyOfRange(decodedNodeId, 22, 23);
        ArrayUtils.reverse(decodedProtocol);
        byte[] decodedSeq = Arrays.copyOfRange(decodedNodeId, 23, 27);
        ArrayUtils.reverse(decodedSeq);
        List<String> strNodeInfo = new ArrayList<String>();
        strNodeInfo.add(Hex.encodeHexString(decodedType));
        strNodeInfo.add(Hex.encodeHexString(decodedId));
        strNodeInfo.add(Hex.encodeHexString(decodedSndIp));
        strNodeInfo.add(Hex.encodeHexString(decodedRcvIp));
        strNodeInfo.add(Hex.encodeHexString(decodedSndPort));
        strNodeInfo.add(Hex.encodeHexString(decodedRcvPort));
        strNodeInfo.add(Hex.encodeHexString(decodedProtocol));
        strNodeInfo.add(Hex.encodeHexString(decodedSeq));
        return strNodeInfo;
    }

    public List<String> decodeNode (byte[] decodedNodeId) {
        byte[] decodedType = Arrays.copyOfRange(decodedNodeId, 0, 8);
        ArrayUtils.reverse(decodedType);
        byte[] decodedId = Arrays.copyOfRange(decodedNodeId, 8, 16);
        ArrayUtils.reverse(decodedId);
        byte[] decodedBootId = Arrays.copyOfRange(decodedNodeId, 16, 20);
        ArrayUtils.reverse(decodedBootId);
        byte[] decodedMachineId = Arrays.copyOfRange(decodedNodeId, 20, 24);
        ArrayUtils.reverse(decodedMachineId);
        byte[] decodedVersion = Arrays.copyOfRange(decodedNodeId, 24, 28);
        ArrayUtils.reverse(decodedVersion);
        List<String> strNodeInfo = new ArrayList<String>();
        strNodeInfo.add(Hex.encodeHexString(decodedType));
        strNodeInfo.add(Hex.encodeHexString(decodedId));
        strNodeInfo.add(Hex.encodeHexString(decodedBootId));
        strNodeInfo.add(Hex.encodeHexString(decodedMachineId));
        strNodeInfo.add(Hex.encodeHexString(decodedVersion));
        return strNodeInfo;
    }

    public List<String> decodeRelation (byte[] decodedRelationId) {
        byte[] decodedType = Arrays.copyOfRange(decodedRelationId, 0, 8);
        ArrayUtils.reverse(decodedType);
        byte[] decodedId = Arrays.copyOfRange(decodedRelationId, 8, 16);
        ArrayUtils.reverse(decodedId);
        byte[] decodedBootId = Arrays.copyOfRange(decodedRelationId, 16, 20);
        ArrayUtils.reverse(decodedBootId);
        byte[] decodedMachineId = Arrays.copyOfRange(decodedRelationId, 20, 24);
        ArrayUtils.reverse(decodedMachineId);
        List<String> strRelationInfo = new ArrayList<String>();
        strRelationInfo.add(Hex.encodeHexString(decodedType));
        strRelationInfo.add(Hex.encodeHexString(decodedId));
        strRelationInfo.add(Hex.encodeHexString(decodedBootId));
        strRelationInfo.add(Hex.encodeHexString(decodedMachineId));
        return strRelationInfo;
    }
}
