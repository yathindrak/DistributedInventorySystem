package cw2.communication.distributedTxProtocol.listener;

public interface TwoPhaseCommitListener {
    void onGlobalCommit();

    void onGlobalAbort();
}

