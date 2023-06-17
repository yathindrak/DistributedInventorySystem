package cw2.communication.nameServiceModule;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;

/**
 * Name service who keeps track of server details using Etcd
 */
public class NameServiceClient {

    private final EtcdClient etcdClient;

    public NameServiceClient(String nameServiceAddress) throws IOException {
        etcdClient = new EtcdClient(nameServiceAddress);
    }

    /**
     * Build an entry for the service
     * @param address - server address
     * @param port - server port
     * @param protocol - server protocol
     * @return JSON object representing the server
     */
    public static String buildServerEntry(String address, int port, String protocol) {
        return new JSONObject()
                .put("ip", address)
                .put("port", Integer.toString(port))
                .put("protocol", protocol)
                .toString();
    }

    /**
     * Find a services via etcd reference
     * @return services
     * @throws InterruptedException
     * @throws IOException
     */
    public ArrayList<Service> findService() throws InterruptedException, IOException {
        System.out.println("Searching for services");
        String etcdResponse = etcdClient.getAll();
        ArrayList<Service> service = populate(etcdResponse);

        while (service == null) {
            System.out.println("Couldn't find the requested service, retrying in 5 seconds.");
            Thread.sleep(5000);
            etcdResponse = etcdClient.getAll();
            service = populate(etcdResponse);
        }

        return service;
    }

    /**
     * Register a service by connection details
     * @param serviceName - name of the service
     * @param IPAddress - ip address of the service
     * @param port - port of the service
     * @param protocol - protocol of the service
     * @throws IOException
     */
    public void registerService(String serviceName, String IPAddress, int port, String protocol) throws IOException {
        String serviceInfoValue = buildServerEntry(IPAddress, port, protocol);
        etcdClient.put(serviceName, serviceInfoValue);
    }

    /**
     * Populate the services
     * @param serverResponse - etcd response
     * @return list of services
     */
    public ArrayList<Service> populate(String serverResponse) {
        ArrayList<Service>  serviceDetails = new ArrayList<>();
        JSONObject serverResponseJSONObject = new JSONObject(serverResponse);

        if (serverResponseJSONObject.has("kvs")) {
            JSONArray values = serverResponseJSONObject.getJSONArray("kvs");
            for(int i = 0; i <values.length(); i++){
                JSONObject value = values.getJSONObject(i);
                String encodedValue = (String)value.get("value");
                byte[] serverDetailsBytes = Base64.getDecoder().decode(encodedValue.getBytes(StandardCharsets.UTF_8));
                JSONObject serverDetailsJson = new JSONObject(new String(serverDetailsBytes));
                Service serviceDetail = new Service(
                        serverDetailsJson.get("ip").toString(),
                        Integer.parseInt(serverDetailsJson.get("port").toString()),
                        serverDetailsJson.get("protocol").toString()
                );
                serviceDetails.add(serviceDetail);
            }

            return serviceDetails;
        } else {
            return null;
        }
    }

    /**
     * Model for the service entity
     */
    public class Service {
        private final String IPAddress;
        private final int port;
        private final String protocol;

        public Service(String IPAddress, int port, String protocol){
            this.IPAddress = IPAddress;
            this.port = port;
            this.protocol = protocol;
        }

        public String getIPAddress() {
            return IPAddress;
        }

        public int getPort() {
            return port;
        }

        public String getProtocol() {
            return protocol;
        }
    }
}
