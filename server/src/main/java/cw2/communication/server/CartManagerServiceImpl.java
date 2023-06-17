package cw2.communication.server;

import com.google.protobuf.Empty;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Implement cart manager grpc service
 */
public class CartManagerServiceImpl extends CartManagerServiceGrpc.CartManagerServiceImplBase
        implements TwoPhaseCommitListener {

    private final InventoryServer server;
    private CartRequest tempTransaction;
    private boolean isOrderFromClient = false;
    private StreamObserver<CartResponse> responseObserver;

    CartManagerServiceGrpc.CartManagerServiceBlockingStub clientStub = null;

    /**
     * CartManagerServiceImpl Constructor.
     *
     * @param server server object.
     */
    public CartManagerServiceImpl(InventoryServer server) {
        this.server = server;
        String[] primaryData = this.server.getCurrentPrimaryData();
        if (primaryData != null && primaryData.length > 0 && !this.server.isPrimary()) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(primaryData[0],
                    Integer.parseInt(primaryData[1])).usePlaintext().build();
            clientStub = CartManagerServiceGrpc.newBlockingStub(channel);
        }
    }

    /**
     * Sends out the data to the primary.
     * This gets used by the secondary servers in forwarding whatever the data they received
     * from clients into the primary server.
     *
     * @param request The cart request.
     */
    private void sendDataToPrimary(CartRequest request) {
        System.out.println("Sending incoming data to primary server...");
        String[] currentLeaderData = server.getCurrentPrimaryData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        CartRequest newRequest = CartRequest
                .newBuilder()
                .setId(request.getId())
                .setUserId(request.getUserId())
                .setQuantity(request.getQuantity())
                .setIsSentByPrimary(false)
                .setIsSentBySecondary(true)
                .build();

        sendDataToServer(newRequest, IPAddress, port);
    }

    /**
     * Sends out data to a specific server.
     *
     * @param request   Data needs to be sent.
     * @param ipAddress Server IP address.
     * @param port      The port where the service is running.
     */
    private void sendDataToServer(CartRequest request, String ipAddress, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(ipAddress, port).usePlaintext().build();
        clientStub = CartManagerServiceGrpc.newBlockingStub(channel);

        CartResponse response = clientStub.addToCart(request);
    }

    /**
     * This method sends order data to all the secondary servers.
     * This method is used by the primary server to send the data it received to all the secondary servers.
     *
     * @param request The order data to be sent.
     * @throws KeeperException      Exception thrown by Zookeeper.
     * @throws InterruptedException Exception thrown when secondary server(s) data is retrieved.
     */
    private void sendDataToSecondaryServers(CartRequest request) throws KeeperException, InterruptedException {
        System.out.println("Sending data to the secondary servers...");
        List<String[]> secondaryServers = server.getAllServersData();

        for (String[] secondaryServer : secondaryServers) {
            String ipAddress = secondaryServer[0];
            int port = Integer.parseInt(secondaryServer[1]);
            sendDataToServer(request, ipAddress, port);
        }
    }

    /**
     * addToCart grpc method implementation.
     *
     * @param request          The cart request.
     * @param responseObserver The stream observer that uses to return data.
     */
    @Override
    public void addToCart(CartRequest request, StreamObserver<CartResponse> responseObserver) {
        if (server.isPrimary()) {
            // If the server is the primary server.
            try {
                System.out.println("Updating trade order as the primary server...");
                CartRequest newRequest = CartRequest
                        .newBuilder()
                        .setId(request.getId())
                        .setUserId(request.getUserId())
                        .setQuantity(request.getQuantity())
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
                if (request.getQuantity() > 0) {
                    ((TwoPhaseCommitCoordinator) server.getCartManagerTransaction()).perform();
                } else {
                    ((TwoPhaseCommitCoordinator) server.getCartManagerTransaction()).sendGlobalAbort();
                }

                if (request.getIsSentBySecondary()) {
                    responseObserver.onNext(CartResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }
            } catch (Exception e) {
                System.out.println("Error while updating cart: " + e.getMessage());
                e.printStackTrace();

                responseObserver.onError(Status.INTERNAL.withDescription("Error occurred, updating cart").asRuntimeException());
            }
        } else {
            // If the server is a secondary server.
            if (request.getIsSentByPrimary()) {
                // If the data is sent by the primary server.
                System.out.println("Updating request on the secondary server, based " +
                        "on the primary server instruction...");

                startTwoPhaseCommit(request);

                // Start two-phase commit
                if (request.getQuantity() > 0) {
                    ((TwoPhaseCommitParticipant) server.getCartManagerTransaction()).voteCommit();
                } else {
                    ((TwoPhaseCommitParticipant) server.getCartManagerTransaction()).voteAbort();
                }

                responseObserver.onNext(CartResponse.newBuilder().build());
                responseObserver.onCompleted();
            } else {
                // If the data is sent by a client.
                this.isOrderFromClient = true;
                this.responseObserver = responseObserver;
                sendDataToPrimary(request);
            }
        }
    }

    @Override
    public void getItems(Empty request, StreamObserver<GetItemsResponse> responseObserver) {
        Map<String, Double> items = server.getInventoryProducts();

        List<ItemRequest> inventoryList = new ArrayList<>();
        for (Map.Entry<String, Double> entry : items.entrySet()) {
            ItemRequest inventoryProduct = ItemRequest.newBuilder()
                    .setId(entry.getKey())
                    .setQuantity(entry.getValue())
                    .build();
            inventoryList.add(inventoryProduct);
        }

        GetItemsResponse response = GetItemsResponse.newBuilder()
                .addAllItems(inventoryList)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    /**
     * Writes cart request to the memory store.
     *
     * @param request The order request.
     */
    private void writeTransaction(CartRequest request) {
        server.setCartItemQuantity(request.getUserId(), request.getId(), request.getQuantity());
    }

    /**
     * Commits an order.
     */
    private void commitTransaction() {
        System.out.println("Committing transaction...");
        if (tempTransaction != null) {
            System.out.println("Writing transaction...");
            writeTransaction(tempTransaction);
            if (!(tempTransaction.getIsSentBySecondary() || (tempTransaction.getIsSentByPrimary() && !this.isOrderFromClient))) {

                isOrderFromClient = false;

                if (this.responseObserver != null) {
                    System.out.println("Return response to the client...");
                    CartResponse responseToClient = CartResponse.newBuilder().setResponse("Cart updated successfully!").build();
                    this.responseObserver.onNext(responseToClient);
                    this.responseObserver.onCompleted();
                }
            }
            tempTransaction = null;

        }
    }

    /**
     * Called on the global commit message.
     */
    @Override
    public void onGlobalCommit() {
        commitTransaction();
    }

    /**
     * Called on the global abort message.
     */
    @Override
    public void onGlobalAbort() {
        String response = "Transaction Aborted by the Coordinator";
        System.out.println(response);
        if (!(tempTransaction.getIsSentBySecondary()
                || (tempTransaction.getIsSentByPrimary() && !this.isOrderFromClient))) {
            isOrderFromClient = false;
            if (this.responseObserver != null) {
                CartResponse responseToClient = CartResponse.newBuilder().setResponse(response).build();
                this.responseObserver.onNext(responseToClient);
                this.responseObserver.onCompleted();
            }
        }
        tempTransaction = null;
    }

    /**
     * Starts the two-phase commit.
     *
     * @param request Request.
     */
    private void startTwoPhaseCommit(CartRequest request) {
        try {
            server.getCartManagerTransaction().start(request.getUserId(), String.valueOf(UUID.randomUUID()));
            tempTransaction = request;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
