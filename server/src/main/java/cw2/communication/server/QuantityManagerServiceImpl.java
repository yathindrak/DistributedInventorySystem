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
import java.util.UUID;

/**
 * Implement quantity manager grpc service
 */
public class QuantityManagerServiceImpl extends QuantityManagerServiceGrpc.QuantityManagerServiceImplBase
        implements TwoPhaseCommitListener {

    private final InventoryServer server;
//    private PlaceOrderRequest tempOrder;

    private UpdateQuantityRequest tempQuantityUpdate;
    private boolean initiatedFromClient = false;

    private StreamObserver<UpdateQuantityResponse> responseObserver;

    QuantityManagerServiceGrpc.QuantityManagerServiceBlockingStub clientStub = null;

    /**
     * QuantityManagerServiceImpl Constructor.
     *
     * @param server server object.
     */
    public QuantityManagerServiceImpl(InventoryServer server) {
        this.server = server;
        String[] primaryData = this.server.getCurrentPrimaryData();
        if (primaryData != null && primaryData.length > 0 && !this.server.isPrimary()) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(primaryData[0],
                    Integer.parseInt(primaryData[1])).usePlaintext().build();
            clientStub = QuantityManagerServiceGrpc.newBlockingStub(channel);
        }
    }

    /**
     * Sends out the data to the primary.
     * This gets used by the secondary servers in forwarding whatever the data they received
     * from clients into the primary server.
     *
     * @param request The update quantity request.
     */
    private void sendDataToPrimary(UpdateQuantityRequest request) {
        System.out.println("Sending received data to the primary server...");
        String[] currentLeaderData = server.getCurrentPrimaryData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        UpdateQuantityRequest newRequest = UpdateQuantityRequest
                .newBuilder()
                .setQuantity(request.getQuantity())
                .setIsSentByPrimary(false)
                .setIsSentBySecondary(true)
                .setId(request.getId())
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
    private void sendDataToServer(UpdateQuantityRequest request, String ipAddress, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(ipAddress, port).usePlaintext().build();
        clientStub = QuantityManagerServiceGrpc.newBlockingStub(channel);

        UpdateQuantityResponse response = clientStub.updateQuantity(request);
    }

    /**
     * Sends data to all the secondary servers registered
     *
     * @param request data.
     * @throws KeeperException      Exception thrown by Zookeeper.
     * @throws InterruptedException Exception thrown when secondary servers data is obtained.
     */
    private void sendDataToSecondaryServers(UpdateQuantityRequest request) throws KeeperException, InterruptedException {
        System.out.println("Sending data to the secondary servers...");
        List<String[]> secondaryServers = server.getAllServersData();

        for (String[] secondaryServer : secondaryServers) {
            String ipAddress = secondaryServer[0];
            int port = Integer.parseInt(secondaryServer[1]);
            sendDataToServer(request, ipAddress, port);
        }
    }

    /**
     * Grpc method for updating the quantity.
     *
     * @param request          Request.
     * @param responseObserver Stream observer used to return data.
     */
    @Override
    public void updateQuantity(UpdateQuantityRequest request, StreamObserver<UpdateQuantityResponse> responseObserver) {
        if (server.isPrimary()) {
            // If the server is the primary server.
            try {
                System.out.println("Updating trade order as the primary server...");
                UpdateQuantityRequest newRequest = UpdateQuantityRequest
                        .newBuilder()
                        .setQuantity(request.getQuantity())
                        .setIsSentByPrimary(true)
                        .setIsSentBySecondary(false)
                        .setId(request.getId())
                        .build();

                startTwoPhaseCommit(request);
                sendDataToSecondaryServers(newRequest);

                if (!request.getIsSentBySecondary()) {
                    this.responseObserver = responseObserver;
                    this.initiatedFromClient = true;
                }

                // Start two-phase commit
                if (request.getQuantity() > 0) {
                    ((TwoPhaseCommitCoordinator) server.getQtyManagerTransaction()).perform();
                } else {
                    ((TwoPhaseCommitCoordinator) server.getQtyManagerTransaction()).sendGlobalAbort();
                }

                if (request.getIsSentBySecondary()) {
                    responseObserver.onNext(UpdateQuantityResponse.newBuilder().build());
                    responseObserver.onCompleted();
                }
            } catch (Exception e) {
                System.out.println("Error while updating trade order: " + e.getMessage());
                e.printStackTrace();

                responseObserver.onError(Status.INTERNAL.withDescription("Error occurred, updating quantity").asRuntimeException());
            }
        } else {
            // If the server is a secondary server.
            if (request.getIsSentByPrimary()) {
                // If the data is sent by the primary server.
                System.out.println("Updating trade order on a secondary server, " +
                        "on the primary server's instruction...");

                startTwoPhaseCommit(request);

                // Start two-phase commit
                if (request.getQuantity() > 0) {
                    ((TwoPhaseCommitParticipant) server.getQtyManagerTransaction()).voteCommit();
                } else {
                    ((TwoPhaseCommitParticipant) server.getQtyManagerTransaction()).voteAbort();
                }

                responseObserver.onNext(UpdateQuantityResponse.newBuilder().build());
                responseObserver.onCompleted();
            } else {
                // If the data is sent by a client.
                this.initiatedFromClient = true;
                this.responseObserver = responseObserver;
                sendDataToPrimary(request);
            }
        }
    }

    /**
     * Write the transaction to the in memory data store.
     *
     * @param request  request.
     */
    private void writeTransaction(UpdateQuantityRequest request) {
        server.setItemQuantity(request.getId(), request.getQuantity());
    }

    /**
     * Commits the transaction.
     */
    private void commitTransaction() {

        if (tempQuantityUpdate != null) {
            System.out.println("Writing order...");
            writeTransaction(tempQuantityUpdate);
            if (!(tempQuantityUpdate.getIsSentBySecondary() || (tempQuantityUpdate.getIsSentByPrimary() && !this.initiatedFromClient))) {

                initiatedFromClient = false;

                if (this.responseObserver != null) {
                    System.out.println("Return response to the client...");
                    UpdateQuantityResponse responseToClient = UpdateQuantityResponse.newBuilder().setResponse(tempQuantityUpdate.getId() + " updated successfully!").build();
                    this.responseObserver.onNext(responseToClient);
                    this.responseObserver.onCompleted();
                }
            }
            tempQuantityUpdate = null;

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
        if (!(tempQuantityUpdate.getIsSentBySecondary()
                || (tempQuantityUpdate.getIsSentByPrimary() && !this.initiatedFromClient))) {
            initiatedFromClient = false;
            if (this.responseObserver != null) {
                UpdateQuantityResponse responseToClient = UpdateQuantityResponse.newBuilder().setResponse(response).build();
                this.responseObserver.onNext(responseToClient);
                this.responseObserver.onCompleted();
            }
        }
        tempQuantityUpdate = null;
    }

    /**
     * Starts the two-phase commit.
     *
     * @param request request.
     */
    private void startTwoPhaseCommit(UpdateQuantityRequest request) {
        try {
            server.getQtyManagerTransaction().start(request.getId(), String.valueOf(UUID.randomUUID()));
            tempQuantityUpdate = request;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
