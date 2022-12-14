package edu.sjsu.cs185c.here;

import java.util.ArrayList;
import java.util.concurrent.PriorityBlockingQueue;
import edu.sjsu.cs249.chain.StateTransferRequest;
import edu.sjsu.cs249.chain.UpdateRequest;
import edu.sjsu.cs249.chain.UpdateResponse;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaBlockingStub;

public class UpdateRequestManager {
    private class ComparableRequest implements Comparable<ComparableRequest> {
        UpdateRequest req;

        public ComparableRequest(UpdateRequest _req) {
            this.req = _req;
        }

        @Override
        public int compareTo(ComparableRequest o) {
            return Integer.compare(req.getXid(), o.req.getXid());
        }
    }

    public ReplicaBlockingStub successorStub;
    private int currentTxId;
    private int lastDeliveredTxId;
    private final PriorityBlockingQueue<ComparableRequest> pendingList = new PriorityBlockingQueue<>();
    private final ArrayList<UpdateRequest> sentList = new ArrayList<UpdateRequest>();

    public synchronized void addUpdateRequest(UpdateRequest req) {
        currentTxId = Integer.max(currentTxId, req.getXid());
        pendingList.add(new ComparableRequest(req));
    }

    public synchronized int getTxId() {
        return currentTxId;
    }

    public synchronized ArrayList<UpdateRequest> getSentList() {
        return sentList;
    }

    public synchronized void incrementTxId() {
        currentTxId = getTxId() + 1;
    }

    public synchronized void stateTransfer(StateTransferRequest request) {
        pendingList.clear(); // may have to comment out
        sentList.clear();
        for (UpdateRequest req : request.getSentList()) {
            addUpdateRequest(req);
        }
        currentTxId = request.getXid();
    }

    public synchronized UpdateRequest peekPendingUpdate() {
        if (pendingList.peek() == null) {
            return null;
        }
        return pendingList.peek().req;
    }

    public synchronized UpdateRequest popPendingUpdate() {
        if (pendingList.poll() == null) {
            return null;
        }
        return pendingList.poll().req;
    }

    // -1: no response
    // 0: can't send yet
    // 1: success
    public synchronized int executeRPC(UpdateRequest req) throws Exception {
        if ((lastDeliveredTxId + 1) != req.getXid()) {
            System.out.println("Cannot forward request because last delivered xid is " + lastDeliveredTxId
                    + ", but request's xid is " + req.getXid());
            return 0;
        }
        if (successorStub == null) {
            return -1;
        }
        UpdateResponse res = successorStub.update(req);
        if (res != null) {
            lastDeliveredTxId = req.getXid();
            sentList.add(req);
        }
        return res == null ? -1 : 1;
    }

    public synchronized void removeFromSentList(int txId) {
        UpdateRequest itemToRemove = null;
        for (UpdateRequest item : sentList) {
            if (item.getXid() == txId) {
                itemToRemove = item;
            }
        }
        if (itemToRemove != null) {
            sentList.remove(itemToRemove);
        }
    }

}
