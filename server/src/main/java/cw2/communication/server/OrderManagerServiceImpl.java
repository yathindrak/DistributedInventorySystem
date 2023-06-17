package cw2.communication.server;

import cw2.communication.grpc.generated.*;
import cw2.communication.distributedTxProtocol.coordinator.TwoPhaseCommitCoordinator;
import cw2.communication.distributedTxProtocol.listener.TwoPhaseCommitListener;
import cw2.communication.distributedTxProtocol.participant.TwoPhaseCommitParticipant;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Implement order manager grpc service
 */
public class OrderManagerServiceImpl extends OrderManagerServiceGrpc.OrderManagerServiceImplBase
        implements TwoPhaseCommitListener {

    private final InventoryServer server;
    private PlaceOrderRequest tempOrder;
    private boolean isOrderFromClient = false;
    private StreamObserver<PlaceOrderResponse> responseObserver;

    OrderManagerServiceGrpc.OrderManagerServiceBlockingStub clientStub = null;

    /**
     * OrderManagerServiceImpl Constructor.
     *
     * @param server The server object.
     */
    public OrderManagerServiceImpl(InventoryServer server) {
        this.server = server;
        String[] primaryData = this.server.getCurrentPrimaryData();
        if (primaryData != null && primaryData.length > 0 && !this.server.isPrimary()) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(primaryData[0],
                    Integer.parseInt(primaryData[1])).usePlaintext().build();
            clientStub = OrderManagerServiceGrpc.newBlockingStub(channel);
        }
    }

    /**
     * Sends out the data to the primary server.
     * This is used by secondary servers to forward the data they
     * receive from clients to the primary server.
     *
     * @param request The order data to be sent.
     */
    private void sendDataToPrimary(PlaceOrderRequest request) {
        System.out.println("Sending received data to the primary server...");
        String[] currentLeaderData = server.getCurrentPrimaryData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        PlaceOrderRequest newRequest = PlaceOrderRequest
                .newBuilder()
                .setUserId(request.getUserId())
                .setIsSentByPrimary(false)
                .setIsSentBySecondary(true)
                .build();

        sendDataToServer(newRequest, IPAddress, port);
    }

    /**
     * Sends request to a server IP address.
     *
     * @param request   Request object
     * @param ipAddress Server IP address.
     * @param port      Serer port.
     */
    private void sendDataToServer(PlaceOrderRequest request, String ipAddress, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(ipAddress, port).usePlaintext().build();
        clientStub = OrderManagerServiceGrpc.newBlockingStub(channel);

        PlaceOrderResponse response = clientStub.placeOrder(request);
    }

    /**
     * Sends data to all the secondary servers registered
     *
     * @param request data.
     * @throws KeeperException      Exception thrown by Zookeeper.
     * @throws InterruptedException Exception thrown when secondary servers data is obtained.
     */
    private void sendDataToSecondaryServers(PlaceOrderRequest request) throws KeeperException, InterruptedException {
        System.out.println("Sending data to the secondary servers...");
        List<String[]> secondaryServers = server.getAllServersData();

        for (String[] secondaryServer : secondaryServers) {
            String ipAddress = secondaryServer[0];
            int port = Integer.parseInt(secondaryServer[1]);
            sendDataToServer(request, ipAddress, port);
        }
    }

    /**
     * Is the user cart items are available to buy in the inventory
     * @param userId
     * @return
     */
    private boolean isProcessable(String userId) {
        boolean isProcessable = true;
        System.out.println("userId : "+ userId);
        Map<String, Double> userCart = server.getUserCart(userId);
        System.out.println("userCart : "+ userCart);

        for (Map.Entry<String, Double> entry : userCart.entrySet()) {
            System.out.println("entry : " + entry);
            String itemId = entry.getKey();
            Double qty = entry.getValue();

            double availableQuantity = server.getItemQuantity(itemId);
            System.out.println("availableQuantity : " + availableQuantity);
            if(availableQuantity < qty) {
                isProcessable = false;
            }
        }

        return isProcessable;
    }

    /**
     * Grpc method for placing the order.
     *
     * @param request          Request.
     * @param responseObserver Stream observer used to return data.
     */
    @Override
    public void placeOrder(PlaceOrderRequest request, StreamObserver<PlaceOrderResponse> responseObserver) {
        System.out.println("placing order...");
        if (server.isPrimary()) {
            // If the server is the primary server.
            try {
                System.out.println("Updating trade order as the primary server...");
                PlaceOrderRequest newRequest = PlaceOrderRequest
                        .newBuilder()
                        .setUserId(request.getUserId())
                        .setIsSentByPrimary(true)
                        .setIsSentBySecondary(false)
                        .build();

                startTwoPhaseCommit(request);
                sendDataToSecondaryServers(newRequest);

                if (!request.getIsSentBySecondary()) {
                    this.responseObserver = responseObserver;
                    this.isOrderFromClient = true;
                }

                // Start two-phase commit
                if (isProcessable(request.getUserId())) {
                    ((TwoPhaseCommitCoordinator) server.getOrderManagerTransaction()).perform();
                } else {
                    ((TwoPhaseCommitCoordinator) server.getOrderManagerTransaction()).sendGlobalAbort();
                }

                if (request.getIsSentBySecondary()) {
                    responseObserver.onNext(PlaceOrderResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }
            } catch (Exception e) {
                System.out.println("Error while updating order: " + e.getMessage());
                e.printStackTrace();

                responseObserver.onError(Status.INTERNAL.withDescription("Error occurred, placing order").asRuntimeException());

            }
        } else {
            // If the server is a secondary server.
            if (request.getIsSentByPrimary()) {
                // If the data is sent by the primary server.
                System.out.println("Updating trade order on a secondary server, " +
                        "on the primary server's instruction...");

                startTwoPhaseCommit(request);

                // Start two-phase commit
                if (isProcessable(request.getUserId())) {
                    ((TwoPhaseCommitParticipant) server.getOrderManagerTransaction()).voteCommit();
                } else {
                    ((TwoPhaseCommitParticipant) server.getOrderManagerTransaction()).voteAbort();
                }

                responseObserver.onNext(PlaceOrderResponse.newBuilder().build());
                responseObserver.onCompleted();
            } else {
                // If the data is sent by a client.
                this.isOrderFromClient = true;
                this.responseObserver = responseObserver;
                sendDataToPrimary(request);
            }
        }
    }


    /**
     * Write the order to the in memory data store.
     *
     * @param request  request.
     */
    private void writeOrder(PlaceOrderRequest request) {
        boolean isProcessable = isProcessable(request.getUserId());
        Map<String, Double> userCart = server.getUserCart(request.getUserId());

        if(isProcessable) {
            for (Map.Entry<String, Double> entry : userCart.entrySet()) {
                String itemId = entry.getKey();
                Double qty = entry.getValue();

                double availableQuantity = server.getItemQuantity(itemId);
                server.setItemQuantity(itemId, availableQuantity - qty);
            }
        }
    }

    /**
     * Commits the order.
     */
    private void commitOrder() {
        System.out.println("Committing order...");
        if (tempOrder != null) {
            System.out.println("Writing order...");
            writeOrder(tempOrder);
            if (!(tempOrder.getIsSentBySecondary() || (tempOrder.getIsSentByPrimary() && !this.isOrderFromClient))) {

                isOrderFromClient = false;

                if (this.responseObserver != null) {
                    System.out.println("Return response to the client...");
                    PlaceOrderResponse responseToClient = PlaceOrderResponse.newBuilder().setResponse(tempOrder.getUserId() + "'s orders are placed successfully!").build();
                    this.responseObserver.onNext(responseToClient);
                    this.responseObserver.onCompleted();
                }
            }
            tempOrder = null;

        }
    }

    /**
     * Called on the global commit message.
     */
    @Override
    public void onGlobalCommit() {
        commitOrder();
    }

    /**
     * Called on the global abort message.
     */
    @Override
    public void onGlobalAbort() {
        String response = "Transaction Aborted by the Coordinator";
        System.out.println(response);
        if (!(tempOrder.getIsSentBySecondary()
                || (tempOrder.getIsSentByPrimary() && !this.isOrderFromClient))) {
            isOrderFromClient = false;
            if (this.responseObserver != null) {
                PlaceOrderResponse responseToClient = PlaceOrderResponse.newBuilder().setResponse(response).build();
                this.responseObserver.onNext(responseToClient);
                this.responseObserver.onCompleted();
            }
        }
        tempOrder = null;
    }

    /**
     * Starts the two-phase commit.
     *
     * @param request request.
     */
    private void startTwoPhaseCommit(PlaceOrderRequest request) {
        try {
            server.getOrderManagerTransaction().start(request.getUserId(), String.valueOf(UUID.randomUUID()));
            tempOrder = request;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
