package cw2.communication.server;

import cw2.communication.distributedTxProtocol.TwoPhaseCommit;
import cw2.communication.distributedTxProtocol.coordinator.TwoPhaseCommitCoordinator;
import cw2.communication.nameServiceModule.NameServiceClient;
import cw2.communication.distributedTxProtocol.participant.TwoPhaseCommitParticipant;
import cw2.communication.primaryBasedProtocol.PrimaryBasedProtocol;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Create and expose the server of the inventory system
 */
public class InventoryServer {
    private static PrimaryBasedProtocol primaryLock;
    private final int serverPort;
    private final AtomicBoolean isPrimary = new AtomicBoolean(false);
    private byte[] primaryData;
    private final OrderManagerServiceImpl orderManagerService;
    private final QuantityManagerServiceImpl quantityManagerService;
    private final CartManagerServiceImpl cartManagerService;
    private TwoPhaseCommit orderManagerTransaction;

    private TwoPhaseCommit qtyManagerTransaction;
    private TwoPhaseCommit cartManagerTransaction;
    public static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";

    // This keeps track of {item_id -> qty} pairs
    private final Map<String, Double> inventoryProducts = new HashMap<>();

    // This keeps track of user_id -> {item_id -> qty} pairs
    private final Map<String, Map<String, Double>> shoppingCart = new HashMap<>();


    /**
     * Constructor.
     *
     * @param host Server IP address.
     * @param port Server port.
     * @throws InterruptedException Interrupt exception.
     * @throws IOException IO exception.
     * @throws KeeperException Zookeeper exception.
     */
    public InventoryServer(String host, int port) throws InterruptedException, IOException, KeeperException {
        this.serverPort = port;
        primaryLock = new PrimaryBasedProtocol("InventorySystem", buildServerData(host, port));
        setCurrentPrimaryData(primaryLock.getPrimaryServerData());
        String[] serverAddress = new String(this.primaryData).split(":");
        if(serverAddress[0].equals(host) && Integer.parseInt(serverAddress[1]) == port){
            isPrimary.set(true);
        }
        quantityManagerService = new QuantityManagerServiceImpl(this);
        orderManagerService = new OrderManagerServiceImpl(this);
        cartManagerService = new CartManagerServiceImpl(this);
        orderManagerTransaction = new TwoPhaseCommitParticipant(orderManagerService);
        qtyManagerTransaction = new TwoPhaseCommitParticipant(quantityManagerService);
        cartManagerTransaction = new TwoPhaseCommitParticipant(cartManagerService);
    }

    /**
     * Bootstrap the server
     */
    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        PrimaryBasedProtocol.setZooKeeperURL("localhost:2181");
        TwoPhaseCommit.setZooKeeperURL("localhost:2181");
        int serverPort = Integer.parseInt(args[0]);

        InventoryServer server = new InventoryServer("localhost", serverPort);
        server.startServer();
    }

    /**
     * Get all products
     * @return map of products
     */
    public Map<String, Double> getInventoryProducts() {
        return inventoryProducts;
    }

    /**
     * Starts the grpc server.
     *
     * @throws IOException IO exception.
     * @throws InterruptedException Interrupt exception.
     */
    public void startServer() throws IOException, InterruptedException {
        Server server = ServerBuilder
                .forPort(serverPort)
                .addService(orderManagerService)
                .addService(quantityManagerService)
                .addService(cartManagerService)
                .build();

        server.start();

        NameServiceClient client = new NameServiceClient(NAME_SERVICE_ADDRESS);
        client.registerService("localhost:" + serverPort, "localhost", serverPort, "tcp");
        System.out.println("Inventory Server is ready to accept client requests on the port " + serverPort);

        tryToBePrimary();
        server.awaitTermination();
    }

    /**
     * Gets the IP  and port of a server
     * then returns a string containing server URI.
     *
     * @param ipAddress Server IP address.
     * @param port Server port.
     * @return Server URI.
     */
    public static String buildServerData(String ipAddress, int port) {
        return ipAddress + ":" + port;
    }

    /**
     * Returns an object that performs a two-phase commit for order mgt.
     *
     * @return two-phase commit type object.
     */
    public TwoPhaseCommit getOrderManagerTransaction() {
        return orderManagerTransaction;
    }

    /**
     * Returns an object that performs a two-phase commit for quantity mgt.
     *
     * @return two-phase commit type object.
     */
    public TwoPhaseCommit getQtyManagerTransaction() {
        return qtyManagerTransaction;
    }

    /**
     * Returns an object that performs a two-phase commit for cart mgt.
     *
     * @return two-phase commit type object.
     */
    public TwoPhaseCommit getCartManagerTransaction() {
        return cartManagerTransaction;
    }

    /**
     * Thread for contesting to be the primary server
     */
    class ContestForPrimaryThread implements Runnable {
        private byte[] currentPrimaryData = null;

        @Override
        public void run() {
            System.out.println("Contesting to become the primary server...");
            try {
                boolean amIPrimary = primaryLock.tryToBeThePrimary();
                while (!amIPrimary) {
                    byte[] primaryData = primaryLock.getPrimaryServerData();
                    if (currentPrimaryData != primaryData) {
                        currentPrimaryData = primaryData;
                        setCurrentPrimaryData(currentPrimaryData);
                    }
                    Thread.sleep(1000);
                    amIPrimary = primaryLock.tryToBeThePrimary();
                }
                System.out.println("Acquired the primary lock. I am the primary server now...");
                isPrimary.set(true);
                orderManagerTransaction = new TwoPhaseCommitCoordinator(orderManagerService);
                cartManagerTransaction = new TwoPhaseCommitCoordinator(cartManagerService);
                qtyManagerTransaction = new TwoPhaseCommitCoordinator(quantityManagerService);
                currentPrimaryData = null;
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * Checks if the current server is the primary server.
     *
     * @return Whether the server is primary server or not.
     */
    public boolean isPrimary() {
        return isPrimary.get();
    }

    /**
     * Sets the current primary data.
     *
     * @param primaryData Primary data.
     */
    private synchronized void setCurrentPrimaryData(byte[] primaryData) {
        this.primaryData = primaryData;
    }

    /**
     * Gets the current primary data.
     *
     * @return Primary data.
     */
    public synchronized String[] getCurrentPrimaryData() {
        if(primaryData != null) {

            return new String(primaryData).split(":");
        }

        return new String[]{};
    }

    /**
     * Gets all the servers' data.
     *
     * @return All the servers' data.
     * @throws KeeperException Zookeeper exception.
     * @throws InterruptedException Interrupt exception.
     */
    public List<String[]> getAllServersData() throws KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> secondaryServersData = primaryLock.getAllServersData();
        for (byte[] data : secondaryServersData) {
            String[] dataStrings = new String(data).split(":");
            result.add(dataStrings);
        }

        return result;
    }

    /**
     * Starting the primary campaign.
     */
    private void tryToBePrimary() {
        Thread primaryCampaignThread = new Thread(new ContestForPrimaryThread());
        primaryCampaignThread.start();
    }

    /**
     * Set item quantity based on the id provided with
     * @param id - item id
     * @param value - quantity
     */
    public void setItemQuantity(String id, double value) {
        inventoryProducts.put(id, value);
    }

    /**
     * Get item quantity by item id
     * @param id - item id
     * @return quantity
     */
    public double getItemQuantity(String id) {
        Double value = inventoryProducts.get(id);
        return (value != null) ? value : 0.0;
    }

    /**
     * Set cart item quantity mapping for a particular user
     * @param userId - user who trying to add to the cart
     * @param itemId - item id
     * @param quantity - quantity of the provided item
     */
    public void setCartItemQuantity(String userId, String itemId, double quantity) {
        // Check if the user already has a record
        if (shoppingCart.containsKey(userId)) {
            Map<String, Double> userRecord = shoppingCart.get(userId);

            // Check if the item already exists in the user's record
            if (userRecord.containsKey(itemId)) {
                // Item exists, append the quantity
                double existingQuantity = userRecord.get(itemId);
                userRecord.put(itemId, existingQuantity + quantity);
            } else {
                // Item doesn't exist, add a new entry
                userRecord.put(itemId, quantity);
            }
        } else {
            // User record doesn't exist, create a new one
            Map<String, Double> userRecord = new HashMap<>();
            userRecord.put(itemId, quantity);
            shoppingCart.put(userId, userRecord);
        }
    }


    /**
     * Get user cart based on the user id
     * @param userId - user id
     * @return user cart
     */
    public Map<String, Double> getUserCart(String userId) {
        return shoppingCart.get(userId);
    }
}
