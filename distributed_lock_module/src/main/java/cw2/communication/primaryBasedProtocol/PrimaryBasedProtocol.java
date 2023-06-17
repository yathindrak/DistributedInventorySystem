package cw2.communication.primaryBasedProtocol;

import cw2.communication.zooKeeper.Client;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class PrimaryBasedProtocol implements Watcher {
    private String childPath;
    private final Client client;
    private String lockPath;
    private boolean isAcquired = false;
    CountDownLatch startFlag = new CountDownLatch(1);
    public static String zooKeeperUrl;
    public byte[] data;

    /**
     * Constructor of PrimaryBasedProtocol.
     *
     * @param lockName zNode label to be created.
     * @param data IP address, port combination of the server.
     * @throws IOException IO exception.
     * @throws KeeperException Zookeeper exception.
     * @throws InterruptedException Interrupt exception.
     */
    public PrimaryBasedProtocol(String lockName, String data) throws IOException, KeeperException, InterruptedException {
        this.lockPath = "/" + lockName;
        this.data = data.getBytes(StandardCharsets.UTF_8);
        client = new Client(zooKeeperUrl, 5000, this);
        startFlag.await();
        if (!client.CheckExists(lockPath)) {
            createRootNode();
        }
        createChildNode();
    }

    /**
     * Sets zookeeper server URL.
     *
     * @param url Zookeeper server URL.
     */
    public static void setZooKeeperURL(String url) {
        zooKeeperUrl = url;
    }

    /**
     * Creates a root node.
     *
     * @throws InterruptedException Interrupt exception.
     * @throws UnsupportedEncodingException Unsupported encoding exception.
     * @throws KeeperException Zookeeper exception.
     */
    private void createRootNode() throws InterruptedException, UnsupportedEncodingException, KeeperException {
        lockPath = client.createNode(lockPath, CreateMode.PERSISTENT,
                "".getBytes(StandardCharsets.UTF_8));

        System.out.println("Root zNode created at " + lockPath);
    }

    /**
     * Creates a child node.
     *
     * @throws InterruptedException Interrupt exception.
     * @throws UnsupportedEncodingException Unsupported encoding exception.
     * @throws KeeperException Zookeeper exception.
     */
    private void createChildNode() throws InterruptedException, UnsupportedEncodingException, KeeperException {
        String lockProcessPath = "/lp_";
        // my_lock_name/lp_{seq_num}
        childPath = client.createNode(lockPath + lockProcessPath,
                CreateMode.EPHEMERAL_SEQUENTIAL, this.data);

        System.out.println("Child zNode created at " + childPath);
    }

    /**
     * Finds the zNode with the smallest sequence.
     *
     * @return Path of the zNode with the smallest sequence.
     * @throws KeeperException Zookeeper exception.
     * @throws InterruptedException Interrupt exception.
     */
    private String findSmallestNodePath() throws KeeperException, InterruptedException {
        List<String> childrenNodePaths = null;
        childrenNodePaths = client.getChildrenNodePaths(lockPath);
        Collections.sort(childrenNodePaths);
        String smallestPath = childrenNodePaths.get(0);
        smallestPath = lockPath + "/" + smallestPath;

        return smallestPath;
    }

    /**
     * Event listener method.
     *
     * @param event listened / watched event.
     */
    @Override
    public void process(WatchedEvent event) {
        Event.KeeperState state = event.getState();
        Event.EventType type = event.getType();
        if (Event.KeeperState.SyncConnected == state) {
            if (Event.EventType.None == type) {
                // Identify successful connection
                System.out.println("Successfully connected to the server.");
                startFlag.countDown();
            }
        }
    }

    /**
     * Retrieve primary server's data.
     *
     * @return data
     * @throws KeeperException Zookeeper exception.
     * @throws InterruptedException Interrupt exception.
     */
    public byte[] getPrimaryServerData() throws KeeperException, InterruptedException {
        String smallestNode = findSmallestNodePath();

        return client.getData(smallestNode, true);
    }

    /**
     * Retrieve all servers data.
     *
     * @return all server data.
     * @throws KeeperException Zookeeper exception.
     * @throws InterruptedException Interrupt exception.
     */
    public List<byte[]> getAllServersData() throws KeeperException, InterruptedException {
        List<byte[]> result = new ArrayList<>();
        List<String> childrenNodePaths = client.getChildrenNodePaths(lockPath);
        for (String path : childrenNodePaths) {
            path = lockPath + "/" + path;
            if (!path.equals(childPath)) {
                byte[] data = client.getData(path, false);
                result.add(data);
            }
        }

        return result;
    }

    /**
     * Tries to become the primary server.
     *
     * @return Whether successful or not.
     * @throws KeeperException Zookeeper exception.
     * @throws InterruptedException Interrupt exception.
     */
    public boolean tryToBeThePrimary() throws KeeperException, InterruptedException {
        String smallestNode = findSmallestNodePath();
        if (smallestNode.equals(childPath)) {
            isAcquired = true;
        }

        return isAcquired;
    }

}
