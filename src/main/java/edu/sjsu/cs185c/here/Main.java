package edu.sjsu.cs185c.here;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import edu.sjsu.cs249.chain.AckRequest;
import edu.sjsu.cs249.chain.AckResponse;
import edu.sjsu.cs249.chain.ChainDebugRequest;
import edu.sjsu.cs249.chain.ChainDebugResponse;
import edu.sjsu.cs249.chain.ExitRequest;
import edu.sjsu.cs249.chain.ExitResponse;
import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.HeadResponse;
import edu.sjsu.cs249.chain.IncRequest;
import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.StateTransferRequest;
import edu.sjsu.cs249.chain.StateTransferResponse;
import edu.sjsu.cs249.chain.UpdateRequest;
import edu.sjsu.cs249.chain.UpdateResponse;
import edu.sjsu.cs249.chain.ChainDebugGrpc.ChainDebugImplBase;
import edu.sjsu.cs249.chain.HeadChainReplicaGrpc.HeadChainReplicaImplBase;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaBlockingStub;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaImplBase;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaImplBase;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class Main {

    // Thread-safe managers
    static final UpdateRequestManager updateManager = new UpdateRequestManager();
    static final AckRequestManager ackManager = new AckRequestManager();
    static Semaphore stubSem = new Semaphore(1);; // protects stubs

    static Cli execObj = null;
    static ZooKeeper zk = null;
    static ConcurrentHashMap<String, Integer> KVStore = new ConcurrentHashMap<>();

    static StreamObserver<HeadResponse> headResponseObserver = null;
    static String myNodeName;

    @Command(name = "zoolunchleader", mixinStandardHelpOptions = true, description = "register attendance for class.")
    static class Cli implements Callable<Integer> {
        @Parameters(index = "0", description = "zookeeper_server_list")
        String serverList;

        @Parameters(index = "1", description = "control_path")
        String controlPath;

        @Parameters(index = "2", description = "full address, ip:hostPort")
        String address;

        @Override
        public Integer call() throws Exception {

            int hostPort = Integer.parseInt(address.split(":")[1]);
            Server server = ServerBuilder.forPort(hostPort).addService(new Head()).addService(new Tail())
                    .addService(new Debug())
                    .addService(new Replica()).build();
            server.start();
            Thread updatesThread = new Thread(new ProcessUpdates(), "Process Updates");
            Thread acksThread = new Thread(new ProcessAcks(), "Process Acks");
            startZookeeper();
            updatesThread.start();
            acksThread.start();
            server.awaitTermination();
            return 0;
        }
    }

    public static void exit() {
        stubSem.release();
        System.exit(0);
    }

    public synchronized static void startZookeeper() { // starting point of program
        try {

            zk = new ZooKeeper(execObj.serverList, 10000, (e) -> {
                if (e.getState() == Watcher.Event.KeeperState.Expired) {
                    exit();
                }

            });

            // Creating my node
            String actualPath = null;
            while (actualPath == null) {
                try {
                    List<String> currentChildren = zk.getChildren(execObj.controlPath, null);
                    boolean imHere = false;
                    // check if my node exists first
                    for (String child : currentChildren) {
                        Stat s = new Stat();
                        zk.getData(execObj.controlPath + "/" + child, null, s);
                        if (s.getEphemeralOwner() == zk.getSessionId()) {
                            imHere = true;
                        }
                    }
                    if (!imHere) {

                        actualPath = zk.create(execObj.controlPath + "/replica-",
                                (execObj.address + "\nujjwal").getBytes(),
                                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

                        String[] splittedPath = actualPath.split("\\/");

                        String myNodeName = splittedPath[splittedPath.length - 1];
                        System.out.println("My name is " + myNodeName);
                        Main.myNodeName = myNodeName;
                        calculateNeighborsAndUpdateStubs();

                    }

                } catch (Exception e) {
                    System.out.println("\nCannot create my own node :(");
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            System.out.println("\nZookeeper Not Found :(");
            e.getStackTrace();
        }

    }

    // caches stubs
    static volatile HashMap<String, ReplicaBlockingStub> nameToStub = new HashMap<>();

    public synchronized static void calculateNeighborsAndUpdateStubs() {
        System.out.println("==========Fetching children==========");
        System.out.println("I am " + myNodeName);

        List<String> currentChildren = null;
        while (currentChildren == null) {
            try {
                currentChildren = zk.getChildren(execObj.controlPath, (e) -> {
                    calculateNeighborsAndUpdateStubs();
                });
            } catch (Exception e) {
                System.out.println("\nCannot fetch children :(");
                e.printStackTrace();
            }
        }

        try {
            stubSem.acquire();
            Collections.sort(currentChildren);
            System.out.println(currentChildren);

            int myIndex = currentChildren.indexOf(myNodeName);
            String myPredecessorName = (myIndex == 0) ? null : currentChildren.get(myIndex - 1);
            String mySuccessorName = (myIndex == (currentChildren.size() - 1)) ? null
                    : currentChildren.get(myIndex + 1);

            if (myPredecessorName == null) {
                System.out.println("I am the head now!");
                ackManager.predecessorStub = null;
            } else {
                if (!nameToStub.containsKey(myPredecessorName)) {
                    try {
                        ReplicaBlockingStub newPredStub = createStub(myPredecessorName);
                        while (newPredStub == null) {
                            System.out.println("trying to create predecessor stub");
                            newPredStub = createStub(myPredecessorName);
                        }
                        nameToStub.put(myPredecessorName, newPredStub);
                    } catch (Exception e) {
                        System.out.println("Error while creating predecessor stub");
                        e.printStackTrace();
                    }
                }
                System.out.println(myPredecessorName + " is my predecessor!");
                ackManager.predecessorStub = nameToStub.get(myPredecessorName);
            }

            if (mySuccessorName == null) {
                System.out.println("I am the tail now!");
                updateManager.successorStub = null;
            } else {
                if (!nameToStub.containsKey(mySuccessorName)) {
                    try {
                        ReplicaBlockingStub newSuccStub = createStub(mySuccessorName);
                        while (newSuccStub == null) {
                            System.out.println("trying to create successor stub");
                            newSuccStub = createStub(mySuccessorName);
                        }
                        nameToStub.put(mySuccessorName, newSuccStub);
                    } catch (Exception e) {
                        System.out.println("Error while creating successor stub");
                        e.printStackTrace();
                    }
                }
                System.out.println(mySuccessorName + " is my successor!");
                ReplicaBlockingStub newSuccessorStub = nameToStub.get(mySuccessorName);
                if (updateManager.successorStub != newSuccessorStub) {
                    // sending state transfer
                    System.out.println("Sending state transfer...");

                    StateTransferRequest req = StateTransferRequest.newBuilder().putAllState(KVStore)
                            .setXid(updateManager.getTxId()).addAllSent(updateManager.getSentList()).build();
                    StateTransferResponse res = newSuccessorStub.stateTransfer(req);
                    while (res == null) {
                        try {
                            res = newSuccessorStub.stateTransfer(req);
                            System.out.println("Trying state transfer");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }
                updateManager.successorStub = newSuccessorStub;
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stubSem.release();
            System.out.println("=====================================");
        }

    }

    public static ReplicaBlockingStub createStub(String nodeName)
            throws Exception {

        byte[] dataBytes = zk.getData(execObj.controlPath + "/" + nodeName, null, null);
        String data = new String(dataBytes);
        String[] splitted = data.split("\n");
        System.out.println(nodeName + " is actually " + splitted[1] + "@" + splitted[0]);

        ManagedChannel newChannel = ManagedChannelBuilder.forTarget(splitted[0])
                .usePlaintext()
                .build();
        return ReplicaGrpc.newBlockingStub(newChannel);

    }

    static public class ProcessUpdates implements Runnable { // make thread and start it, also add prints

        @Override
        public void run() {
            System.out.println("Started processing updates...");
            while (true) {
                UpdateRequest currUpdateReq = updateManager.peekPendingUpdate();
                boolean done = false;
                while (currUpdateReq != null && !done) {
                    try {
                        stubSem.acquire();
                        int rc = updateManager.executeRPC(currUpdateReq);
                        while (updateManager.successorStub != null && rc == -1) {
                            // keep trying if stub exists but does not return a response
                            try {
                                stubSem.release();
                                calculateNeighborsAndUpdateStubs(); // updates the predecessor and successor stubs
                                stubSem.acquire();
                                rc = updateManager.executeRPC(currUpdateReq);
                                System.out.println("trying to send updatereq but failing :(");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        if (updateManager.successorStub == null) {
                            int txId = updateManager.popPendingUpdate().getXid();
                            System.out.println("Assuming I am the tail now, adding ack to buffer for txId: " + txId);
                            AckRequest ack = AckRequest.newBuilder().setXid(txId).build();
                            ackManager.addAckRequest(ack);
                        }
                        if (rc == 1) {
                            int txId = updateManager.popPendingUpdate().getXid();
                            System.out.println("Succesfully forwarded update with txId: " + txId);
                        } else if (rc == 0) {
                            // Not ready to send update yet
                            done = true;
                        }
                        stubSem.release();

                    } catch (Exception e) {
                        stubSem.release();
                        e.printStackTrace();
                    }
                    // see if I can send the next update
                    currUpdateReq = updateManager.peekPendingUpdate();

                }
            }
        }
    }

    static public class ProcessAcks implements Runnable {
        @Override
        public void run() {
            System.out.println("Started processing acks...");
            while (true) {
                AckRequest currAckRequest = ackManager.peekPendingAck();
                boolean done = false;

                while (currAckRequest != null && !done) {
                    try {
                        stubSem.acquire();
                        int rc = ackManager.executeRPC(currAckRequest);
                        while (ackManager.predecessorStub != null && rc == -1) {
                            // keep trying if stub exists but does not return a response
                            try {
                                stubSem.release();
                                calculateNeighborsAndUpdateStubs(); // updates the predecessor and successor stubs
                                stubSem.acquire();
                                rc = ackManager.executeRPC(currAckRequest);
                                System.out.println("trying to send ackreq but failing :(");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        if (ackManager.predecessorStub == null) {
                            int txId = ackManager.popPendingAck().getXid();
                            updateManager.removeFromSentList(txId);

                            System.out.println(
                                    "Assuming I am the head now, sending head response for txId and removing it from sentList: "
                                            + txId);
                            HeadResponse res = HeadResponse.newBuilder().setRc(0).build();
                            headResponseObserver.onNext(res);
                            headResponseObserver.onCompleted();
                        }
                        if (rc == 1) {
                            int txId = ackManager.popPendingAck().getXid();
                            updateManager.removeFromSentList(txId);
                            System.out.println("Successfully forwarded ack and removed from sentList, id: " + txId);
                        } else if (rc == 0) {
                            // Not ready to send ack yet
                            done = true;
                        }
                        stubSem.release();

                    } catch (Exception e) {
                        stubSem.release();
                        e.printStackTrace();
                    }
                    currAckRequest = ackManager.peekPendingAck();
                }
            }

        }
    }

    static class Head extends HeadChainReplicaImplBase {
        @Override
        public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
            System.out.println("\nGot increment request");
            try {
                stubSem.acquire(); // check if i'm actually the head

                headResponseObserver = responseObserver; // used to send incresponse back when ack is returned

                if (ackManager.predecessorStub != null) {
                    System.out.println("\nNot the head");
                    HeadResponse notHead = HeadResponse.newBuilder().setRc(1).build();
                    responseObserver.onNext(notHead);
                    responseObserver.onCompleted();
                } else {
                    if (KVStore.get(request.getKey()) == null) {
                        KVStore.put(request.getKey(), 0);
                    }
                    int newValue = KVStore.get(request.getKey()) + request.getIncValue();
                    KVStore.put(request.getKey(), newValue);
                    updateManager.incrementTxId();
                    UpdateRequest req = UpdateRequest.newBuilder().setKey(request.getKey()).setNewValue(newValue)
                            .setXid(updateManager.getTxId()).build();
                    updateManager.addUpdateRequest(req);
                    System.out.println("added to buffer");
                }
            } catch (Exception e) {
                System.out.println("\n Error while processing an increment request");
                e.printStackTrace();
            } finally {
                stubSem.release();
            }

        }
    }

    static class Tail extends TailChainReplicaImplBase {
        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            System.out.println("\nGot get request");
            try {
                stubSem.acquire(); // check if i am actually the tail
                int val = KVStore.containsKey(request.getKey()) ? KVStore.get(request.getKey()) : 0;
                GetResponse actuallyTail = GetResponse.newBuilder().setRc(updateManager.successorStub == null ? 0 : 1)
                        .setValue(val)
                        .build();
                responseObserver.onNext(actuallyTail);
                responseObserver.onCompleted();
                System.out.println("\nResponded back");

            } catch (Exception e) {
                System.out.println("\nError while processing a get request");
                e.printStackTrace();
            } finally {
                stubSem.release();
            }
        }
    }

    static class Debug extends ChainDebugImplBase {
        @Override
        public void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {
            System.out.println("\nGot debug request");
            try {
                ChainDebugResponse.Builder builder = ChainDebugResponse.newBuilder();

                builder.addAllSent(updateManager.getSentList());
                builder.putAllState(KVStore);
                builder.setXid(updateManager.getTxId());

                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                System.out.println("Error while processing a debug request");
                e.printStackTrace();
            } finally {
                // stubSem.release();
            }

        }

        @Override
        public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
            System.out.println("\nGot exit request");
            responseObserver.onNext(ExitResponse.newBuilder().build());
            responseObserver.onCompleted();

            stubSem.release();
            System.exit(0);
        }
    }

    static class Replica extends ReplicaImplBase {
        @Override
        public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {

            System.out.println("\nGot update request");
            try {
                // idlingSem.acquire();
                System.out.println("\nthe id is: " + request.getXid());

                KVStore.put(request.getKey(), request.getNewValue());
                updateManager.addUpdateRequest(request);
                System.out.println("Added to buffer");
                UpdateResponse res = UpdateResponse.newBuilder().setRc(0).build();
                responseObserver.onNext(res);
                responseObserver.onCompleted();

            } catch (Exception e) {
                System.out.println("\nError while processing an update request");
                e.printStackTrace();
            } finally {
                // idlingSem.release();
            }

        }

        @Override
        public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
            System.out.println("\nGot ack request");
            try {
                // idlingSem.acquire();
                ackManager.addAckRequest(request);
                System.out.println("Added to buffer");
                responseObserver.onNext(AckResponse.newBuilder().build());
                responseObserver.onCompleted();

            } catch (Exception e) {
                System.out.println("\nError while processing an ack request");
                e.printStackTrace();
            } finally {
                // stubSem.release();
                // idlingSem.release();
            }
        }

        @Override
        public void stateTransfer(StateTransferRequest request,
                StreamObserver<StateTransferResponse> responseObserver) {

            System.out.println("\nGot StateTransfer request");
            try {
                if (ackManager.predecessorStub == null) {
                    calculateNeighborsAndUpdateStubs();
                }
                stubSem.acquire();
                Map<String, Integer> recievedKVStore = request.getStateMap();
                for (String key : recievedKVStore.keySet()) {
                    KVStore.put(key, recievedKVStore.get(key));
                }
                updateManager.stateTransfer(request);

                StateTransferResponse thanks = StateTransferResponse.newBuilder().setRc(0).build();
                responseObserver.onNext(thanks);
                responseObserver.onCompleted();
            } catch (

            Exception e) {
                System.out.println("\nError while processing a StateTransfer request");
                e.printStackTrace();
            } finally {
                stubSem.release();
            }
        }

    }

    public static void main(String[] args) {
        execObj = new Cli();
        System.exit(new CommandLine(execObj).execute(args));
    }
}

// 172.27.17.13
// java -jar target/chain_replication-1.48.1-spring-boot.jar
// zookeeper.class.homeofcode.com /newtry 172.27.17.13:2000