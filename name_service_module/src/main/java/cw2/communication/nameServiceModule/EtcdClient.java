package cw2.communication.nameServiceModule;

import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Client for dealing with the etcd data store
 */
public class EtcdClient {
    private final String etcdAddress;

    public EtcdClient(String etcdAddress) {
        this.etcdAddress = etcdAddress;
    }

    /**
     * Put a key value pair
     * @param key - key
     * @param value - value
     * @throws IOException
     */
    public void put(String key, String value) throws IOException {
        System.out.println("Putting Key=" + key + ",Value=" + value);
        String putUrl = etcdAddress + "/v3/kv/put";
        String serverResponse = callEtcd(putUrl, buildPutRequestPayload(key, value));
        System.out.println(serverResponse);
    }

    /**
     * Get an object from the key
     * @param key - key associated with the intended object
     * @return - associated item
     * @throws IOException
     */
    public String get(String key) throws IOException {
        System.out.println("Getting value for Key=" + key);
        String getUrl = etcdAddress + "/v3/kv/range";

        return callEtcd(getUrl, buildGetRequestPayload(key));
    }

    /**
     * Get all the items stored
     * @return all the items
     * @throws IOException
     */
    public String getAll() throws IOException {
        String getUrl = etcdAddress + "/v3/kv/range";

        return callEtcd(getUrl, buildGetRequestPayload());
    }

    /**
     * Make Http request to etcd instance
     * @param url - connection url for etcd instance
     * @param payload - payload need to be sent to etcd
     * @return response
     * @throws IOException
     */
    private String callEtcd(String url, String payload) throws IOException {
        URL etcdUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) etcdUrl.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        connection.connect();
        OutputStream outputStream = connection.getOutputStream();
        outputStream.write(payload.getBytes(StandardCharsets.UTF_8));
        InputStream inputStream = connection.getInputStream();
        String serverResponse = readResponse(inputStream);
        inputStream.close();
        outputStream.close();
        connection.disconnect();

        return serverResponse;
    }

    /**
     * Read response char by char
     * @param inputStream - response as a stream
     * @return - response as a string
     * @throws IOException
     */
    private String readResponse(InputStream inputStream)
            throws IOException {
        StringBuilder builder = new StringBuilder();
        int character = inputStream.read();
        while (character != -1) {
            builder.append((char) character);
            character = inputStream.read();
        }

        return builder.toString();
    }

    /**
     * Build put payload using a key value pair
     * @param key
     * @param value
     * @return
     */
    private String buildPutRequestPayload(String key, String value) {
        String keyEncoded = Base64.getEncoder().encodeToString(key.getBytes());
        String valueEncoded = Base64.getEncoder().encodeToString(value.getBytes());
        JSONObject putRequest = new JSONObject();
        putRequest.put("key", keyEncoded);
        putRequest.put("value", valueEncoded);

        return putRequest.toString();
    }

    /**
     * Build get request payload via the key
     * @param key
     * @return
     */
    private String buildGetRequestPayload(String key) {
        String keyEncoded = Base64.getEncoder().encodeToString(key.getBytes());
        JSONObject putRequest = new JSONObject();
        putRequest.put("key", keyEncoded);

        return putRequest.toString();
    }

    /**
     * Payload for getting all the items in etcd
     * @return
     */
    private String buildGetRequestPayload() {
        String keyEncoded = Base64.getEncoder().encodeToString("\0".getBytes());
        String rangeEncoded = Base64.getEncoder().encodeToString("\0".getBytes());
        JSONObject putRequest = new JSONObject();
        putRequest.put("key", keyEncoded);
        putRequest.put("range_end", rangeEncoded);

        return putRequest.toString();
    }
}