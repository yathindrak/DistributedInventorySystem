package cw2.communication.distributedTxProtocol.coordinator;

import cw2.communication.distributedTxProtocol.listener.TwoPhaseCommitListener;
import cw2.communication.distributedTxProtocol.TwoPhaseCommit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Coordinates two phase commit protocol
 */
public class TwoPhaseCommitCoordinator extends TwoPhaseCommit {

    public TwoPhaseCommitCoordinator(TwoPhaseCommitListener listener) {
        super(listener);
    }

    /**
     * Calls on the transaction start
     * @param orderId
     * @param participantId
     */
    public void onStartTransaction(String orderId, String participantId) {
        try {
            currentOrder = "/" + orderId;
            client.createNode(currentOrder, CreateMode.PERSISTENT, "".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Perform the transaction if there are no any aborts
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean perform() throws KeeperException, InterruptedException {
        List<String> childrenNodePaths = client.getChildrenNodePaths(currentOrder);
        boolean result = true;
        byte[] data;

        System.out.println("Child count :" + childrenNodePaths.size());

        for (String path : childrenNodePaths) {
            path = currentOrder + "/" + path;
            System.out.println("Checking path :" + path);
            data = client.getData(path, false);
            String dataString = new String(data);

            if (!VOTE_COMMIT.equals(dataString)) {
                System.out.println("Child " + path + "caused the order to abort. Sending GLOBAL_ABORT");
                sendGlobalAbort();
                result = false;
            }
        }
        System.out.println("All nodes are okay to commit the order. Sending GLOBAL_COMMIT");
        sendGlobalCommit();
        reset();
        return result;
    }

    /**
     * Send commit message globally
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void sendGlobalCommit() throws KeeperException, InterruptedException {
        if (currentOrder != null) {
            System.out.println("Sending global commit for" + currentOrder);
            client.write(currentOrder, TwoPhaseCommitCoordinator.GLOBAL_COMMIT.getBytes(StandardCharsets.UTF_8));
            listener.onGlobalCommit();
        }
    }

    /**
     * Send abort message globally
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void sendGlobalAbort() throws KeeperException, InterruptedException {
        if (currentOrder != null) {
            System.out.println("Sending global abort for" + currentOrder);
            client.write(currentOrder, TwoPhaseCommitCoordinator.GLOBAL_ABORT.getBytes(StandardCharsets.UTF_8));
            listener.onGlobalAbort();
        }
    }

    /**
     * Reset the lock
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void reset() throws KeeperException, InterruptedException {
        client.forceDelete(currentOrder);
        currentOrder = null;
    }
}



