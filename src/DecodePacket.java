import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
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
}
