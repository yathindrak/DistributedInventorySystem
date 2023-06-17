package cw2.communication.zooKeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * Zookeeper client
 */
public class Client {
    private final ZooKeeper zooKeeper;

    public Client(String zooKeeperUrl, int sessionTimeout, Watcher watcher) throws IOException {
        zooKeeper = new ZooKeeper(zooKeeperUrl, sessionTimeout, watcher);
    }

    /**
     * Creates a node
     * @param path - path that the node should be created
     * @param mode - mode of the node
     * @param data - data to be passed to the node
     * @return path of the new node
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String createNode(String path, CreateMode mode, byte[] data) throws KeeperException,
            InterruptedException {
        return zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    /**
     * Checks whether the node exists in a given path
     * @param path - given path
     * @return - whether node exists
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean CheckExists(String path) throws KeeperException, InterruptedException {
        Stat nodeStat = zooKeeper.exists(path, false);

        return (nodeStat != null);
    }

    /**
     * Deletes a node
     * @param path - path to be deleted
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void delete(String path) throws KeeperException, InterruptedException {
        zooKeeper.delete(path, -1);
    }

    /**
     * Get immediate children for a given path
     * @param root - given path
     * @return list of labels of immediate children
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getChildrenNodePaths(String root) throws KeeperException, InterruptedException {
        return zooKeeper.getChildren(root, false);
    }

    /**
     * Allows watching a given node
     * @param path - path of the node to be listened to
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void addWatch(String path) throws KeeperException, InterruptedException {
        zooKeeper.exists(path, true);
    }

    /**
     * Get data of a given node path
     * @param path - path of the node
     * @param shouldWatch - should the node need to be watched
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] getData(String path, boolean shouldWatch) throws KeeperException, InterruptedException {
        return zooKeeper.getData(path, shouldWatch, null);
    }

    /**
     * Write data to a node path
     * @param path - path to be written the data
     * @param data - data to be written
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void write(String path, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data, -1);
    }

    /**
     * Delete all the child paths recursively; forcefully
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void forceDelete(String path) throws KeeperException, InterruptedException {
        ZKUtil.deleteRecursive(zooKeeper, path);
    }

}
