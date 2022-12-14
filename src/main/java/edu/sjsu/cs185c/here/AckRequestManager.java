package edu.sjsu.cs185c.here;

import java.util.concurrent.PriorityBlockingQueue;

import edu.sjsu.cs249.chain.AckRequest;
import edu.sjsu.cs249.chain.AckResponse;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaBlockingStub;

public class AckRequestManager {
    private class ComparableRequest implements Comparable<ComparableRequest> {
        AckRequest req;

        public ComparableRequest(AckRequest _req) {
            this.req = _req;
        }

        @Override
        public int compareTo(ComparableRequest o) {
            return Integer.compare(req.getXid(), o.req.getXid());
        }
    }

    public ReplicaBlockingStub predecessorStub;
    private int currentTxId;
    private int lastDeliveredTxId;
    private PriorityBlockingQueue<ComparableRequest> buffer = new PriorityBlockingQueue<>();

    public synchronized void addAckRequest(AckRequest req) {
        currentTxId = Integer.max(currentTxId, req.getXid());
        buffer.add(new ComparableRequest(req));
    }

    public synchronized int getTxId() {
        return currentTxId;
    }

    public AckRequest peekPendingAck() {
        if (buffer.peek() == null) {
            return null;
        }
        return buffer.peek().req;
    }

    public AckRequest popPendingAck() {
        if (buffer.poll() == null) {
            return null;
        }
        return buffer.poll().req;
    }

    // -1: no response
    // 0: can't send yet
    // 1: success
    public synchronized int executeRPC(AckRequest req) throws Exception {
        if ((lastDeliveredTxId + 1) != req.getXid()) {
            System.out.println("Cannot send ack request because last delivered xid is " + lastDeliveredTxId
                    + ", but request's xid is " + req.getXid());
            return 0;
        }
        if (predecessorStub == null) {
            return -1;
        }
        AckResponse res = predecessorStub.ack(req);
        if (res != null) {
            lastDeliveredTxId = req.getXid();
        }
        return res == null ? -1 : 1;
    }
}
