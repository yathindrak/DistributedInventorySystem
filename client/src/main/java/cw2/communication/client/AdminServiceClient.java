package cw2.communication.client;

import cw2.communication.grpc.generated.*;
import cw2.communication.nameServiceModule.NameServiceClient;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Client for admin services
 */
public class AdminServiceClient {
    private ManagedChannel channel = null;
    QuantityManagerServiceGrpc.QuantityManagerServiceBlockingStub clientStub = null;
    String host = "localhost"; // default service host
    int port = 5000; // default server port
    public static final String NAME_SERVICE_DEPLOYMENT_ADDR = "http://localhost:2379";

    public AdminServiceClient(String host, int port) throws IOException, InterruptedException {
        this.host = host;
        this.port = port;
        // fetching server information
        fetchServersViaNS();
    }

    /**
     * Fetch server information and try to establish a connection
     * @throws IOException
     * @throws InterruptedException
     */
    private void fetchServersViaNS() throws IOException, InterruptedException {
        NameServiceClient client = new NameServiceClient(NAME_SERVICE_DEPLOYMENT_ADDR);
        ArrayList<NameServiceClient.Service> services = client.findService();

        for (NameServiceClient.Service service : services) {
            host = service.getIPAddress();
            port = service.getPort();
            initializeConnection();
            Thread.sleep(1000);
            ConnectivityState state = channel.getState(true);
            if (state == ConnectivityState.READY) {
                System.out.println("Established connection to the server "+  service.getIPAddress() + ":" + service.getPort() +" successfully.");
                break;
            } else {
                System.out.println("Error in establishing connection.");
            }
        }
    }

    /**
     * Try to initialize the connection to the server and check the state
     */
    private void initializeConnection() {
        System.out.println("Initializing connection to the server under " + host + ":" + port);
        channel = ManagedChannelBuilder.forAddress(host, this.port)
                .usePlaintext()
                .build();
        clientStub = QuantityManagerServiceGrpc.newBlockingStub(channel);
        channel.getState(true);
    }

    /**
     * Close the connection
     */
    void closeConnection() {
        channel.shutdown();
    }

    /**
     * Get user requests and process them accordingly
     */
    void processUserRequests() {
        while (true) {
            try {
                UpdateQuantityRequest quantityRequest = null;
                Scanner userInput = new Scanner(System.in);
                System.out.println("Hey, Admin!");
                System.out.println("Choose one of the actions:");
                System.out.println("1. Update quantity (Enter 1)");
                String action = userInput.nextLine().trim();
                if (Integer.parseInt(action) == 1) {
                    System.out.println("\nEnter item id:");
                    String itemId = userInput.nextLine().trim();
                    System.out.println("\nEnter quantity:");
                    int quantity = Integer.parseInt(userInput.nextLine().trim());

                    quantityRequest = UpdateQuantityRequest
                            .newBuilder()
                            .setId(itemId)
                            .setQuantity(quantity)
                            .setIsSentByPrimary(false)
                            .setIsSentBySecondary(false)
                            .build();
                    ConnectivityState state = channel.getState(true);

                    while (state != ConnectivityState.READY) {
                        System.out.println("Service not available, retrying...");
                        fetchServersViaNS();
                        Thread.sleep(5000);
                        state = channel.getState(true);
                    }

                    UpdateQuantityResponse response = clientStub.updateQuantity(quantityRequest);
                    System.out.println(response.getResponse());
                }

                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}

