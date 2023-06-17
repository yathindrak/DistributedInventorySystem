package cw2.communication.client;

import com.google.protobuf.Empty;
import cw2.communication.grpc.generated.*;
import cw2.communication.nameServiceModule.NameServiceClient;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Client for shopper services
 */
public class ShopperServiceClient {
    private ManagedChannel channel = null;
    CartManagerServiceGrpc.CartManagerServiceBlockingStub cartClientStub = null;
    OrderManagerServiceGrpc.OrderManagerServiceBlockingStub orderClientStub = null;
    String host = "localhost"; // default service host
    int port = 5000; // default server port
    public static final String NAME_SERVICE_DEPLOYMENT_ADDR = "http://localhost:2379";

    public ShopperServiceClient(String host, int port) throws IOException, InterruptedException {
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
        cartClientStub = CartManagerServiceGrpc.newBlockingStub(channel);
        orderClientStub = OrderManagerServiceGrpc.newBlockingStub(channel);
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
                PlaceOrderRequest orderRequest = null;
                CartRequest cartRequest = null;

                Scanner userInput = new Scanner(System.in);
                System.out.println("Hey, Customer!");
                System.out.println("Choose one of the actions:");
                System.out.println("1. Add to cart (Enter 1)");
                System.out.println("2. Place order (Enter 2)");
                String action = userInput.nextLine().trim();
                if (Integer.parseInt(action) == 1) {
                    System.out.println("Enter your id:");
                    String userId = userInput.nextLine().trim();

                    GetItemsResponse itemsResponse = cartClientStub.getItems(Empty.getDefaultInstance());
                    List<ItemRequest> items = itemsResponse.getItemsList();
                    ItemRequest selectedItem = null;
                    do {
                        System.out.println("Choose one of the item ID:");
                        for (ItemRequest item : items) {
                            System.out.println(item.getId());
                        }
                        String id = userInput.nextLine().trim();

                        for (ItemRequest item : items) {
                            if (item.getId().equals(id)) {
                                selectedItem = item;
                                break;
                            }
                        }

                        if (selectedItem == null) {
                            System.out.println("Chosen ID not found. Please select one from the list.");
                        }
                    } while (selectedItem == null);

                    System.out.println("Enter quantity:");
                    int quantity = Integer.parseInt(userInput.nextLine().trim());

                    cartRequest = CartRequest
                            .newBuilder()
                            .setId(selectedItem.getId())
                            .setUserId(userId)
                            .setQuantity(quantity)
                            .setIsSentByPrimary(false)
                            .setIsSentBySecondary(false)
                            .build();

                    ConnectivityState state = channel.getState(true);

                    while (state != ConnectivityState.READY) {
                        System.out.println("Service unavailable, retrying...");
                        fetchServersViaNS();
                        Thread.sleep(5000);
                        state = channel.getState(true);
                    }

                    CartResponse response = cartClientStub.addToCart(cartRequest);
                    System.out.println(response.getResponse());
                } else if (Integer.parseInt(action) == 2) {
                    System.out.println("Enter your id:");
                    String userId = userInput.nextLine().trim();

                    orderRequest = PlaceOrderRequest
                            .newBuilder()
                            .setUserId(userId)
                            .setIsSentByPrimary(false)
                            .setIsSentBySecondary(false)
                            .build();
                    ConnectivityState state = channel.getState(true);

                    while (state != ConnectivityState.READY) {
                        System.out.println("Service unavailable, retrying...");
                        fetchServersViaNS();
                        Thread.sleep(5000);
                        state = channel.getState(true);
                    }

                    PlaceOrderResponse response = orderClientStub.placeOrder(orderRequest);
                    System.out.println(response.getResponse());
                }

                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}

