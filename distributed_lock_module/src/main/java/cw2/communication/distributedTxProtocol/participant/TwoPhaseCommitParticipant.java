package cw2.communication.distributedTxProtocol.participant;

import cw2.communication.distributedTxProtocol.TwoPhaseCommit;
import cw2.communication.distributedTxProtocol.coordinator.TwoPhaseCommitCoordinator;
import cw2.communication.distributedTxProtocol.listener.TwoPhaseCommitListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.nio.charset.StandardCharsets;

/**
 * Participant class for two phase commit protocol
 */
public class TwoPhaseCommitParticipant extends TwoPhaseCommit implements Watcher {

    private static final String PARTICIPANT_PREFIX = "/part_";
    private String orderRoot;

    public TwoPhaseCommitParticipant(TwoPhaseCommitListener listener) {
        super(listener);
    }

    /**
     * Vote for the transaction commit
     */
    public void voteCommit() {
        try {
            if (currentOrder != null) {
                System.out.println("Voting to commit the transaction... " +
                        currentOrder);

                client.write(currentOrder,
                        TwoPhaseCommitCoordinator.VOTE_COMMIT.getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Vote for the transaction abort
     */
    public void voteAbort() {
        try {
            if (currentOrder != null) {
                System.out.println("Voting to abort the transaction... " +
                        currentOrder);
                client.write(currentOrder,
                        TwoPhaseCommitCoordinator.VOTE_ABORT.getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Reset values
     */
    private void reset() {
        currentOrder = null;
        orderRoot = null;
    }

    /**
     * Calls on the transaction start
     * @param orderId
     * @param participantId
     */
    protected void onStartTransaction(String orderId, String participantId) {
        try {
            orderRoot = "/" + orderId;
            currentOrder = orderRoot + PARTICIPANT_PREFIX + participantId;
            client.createNode(currentOrder, CreateMode.EPHEMERAL, "".getBytes(StandardCharsets.UTF_8));
            client.addWatch(orderRoot);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Deals with data changes considering it whether for a commit or abort
     */
    private void handleRootDataChange() {
        try {
            byte[] data = client.getData(orderRoot, true);
            String dataString = new String(data);
            if (TwoPhaseCommitCoordinator.GLOBAL_COMMIT.equals(dataString)) {
                listener.onGlobalCommit();
            } else if (TwoPhaseCommitCoordinator.GLOBAL_ABORT.equals(dataString)) {
                listener.onGlobalAbort();
            } else {
                System.out.println("Unknown data change in the root : " + dataString);
            }
        } catch (Exception e) {
//            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        Event.EventType type = event.getType();
        if (Event.EventType.NodeDataChanged.equals(type)) {
            if (orderRoot != null && event.getPath().equals(orderRoot)) {
                handleRootDataChange();
            }
        }
        if (Event.EventType.NodeDeleted.equals(type)) {
            if (orderRoot != null && event.getPath().equals(orderRoot)) {
                reset();
            }
        }
    }
}


