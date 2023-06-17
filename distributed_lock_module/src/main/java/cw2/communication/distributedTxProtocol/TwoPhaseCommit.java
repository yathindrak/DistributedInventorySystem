package cw2.communication.distributedTxProtocol;

import cw2.communication.distributedTxProtocol.listener.TwoPhaseCommitListener;
import cw2.communication.zooKeeper.Client;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.io.IOException;

/**
 * Abstract two phase commit class
 */
public abstract class TwoPhaseCommit implements Watcher {
    public static final String VOTE_COMMIT = "vote_commit";
    public static final String VOTE_ABORT = "vote_abort";
    public static final String GLOBAL_COMMIT = "global_commit";
    public static final String GLOBAL_ABORT = "global_abort";
    static String zooKeeperUrl;
    public String currentOrder;
    public Client client;
    public TwoPhaseCommitListener listener;

    public TwoPhaseCommit(TwoPhaseCommitListener listener) {
        this.listener = listener;
    }

    /**
     * Sets the zookeeper url
     * @param url - zookeeper url
     */
    public static void setZooKeeperURL(String url) {
        zooKeeperUrl = url;
    }

    /**
     * Start processing the transaction
     * @param trxId
     * @param participantId
     * @throws IOException
     */
    public void start(String trxId, String participantId) throws IOException {
        client = new Client(zooKeeperUrl, 5000,this);
        onStartTransaction(trxId, participantId);
    }

    /**
     * Calls on the transaction start
     * @param orderId
     * @param participantId
     */
    protected abstract void onStartTransaction(String orderId, String participantId);

    @Override
    public void process(WatchedEvent watchedEvent) {
    }
}