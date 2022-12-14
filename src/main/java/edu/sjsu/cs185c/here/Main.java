package edu.sjsu.cs185c.here;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

    static final boolean debug = true;

    // One time usage static references
    static Cli execObj = null;
    static ZooKeeper zk = null;

    // Static references that could change
    static Semaphore idlingSem = null;
    static Semaphore stubSem = null;

    static HashMap<String, Integer> KVStore = new HashMap<>();
    static ArrayList<UpdateRequest> sentList = new ArrayList<UpdateRequest>();
    static int globalTxIDPending = 0;
    static int globalTxIDAcked = 0;
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

            // Initial threadsafe setup
            idlingSem = new Semaphore(1); // exactly one thread can run at once, all others have to wait
            stubSem = new Semaphore(1);
            int hostPort = Integer.parseInt(address.split(":")[1]);
            Server server = ServerBuilder.forPort(hostPort).addService(new Head()).addService(new Tail())
                    .addService(new Debug())
                    .addService(new Replica()).build();
            server.start();
            startZookeeper();
            server.awaitTermination();
            return 0;
        }
    }

    public static void exit() {
        idlingSem.release();
        System.exit(0);
    }

    public static void startZookeeper() { // starting point of program

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
                } finally {
                    idlingSem.release();
                }
            }

        } catch (Exception e) {
            System.out.println("\nZookeeper Not Found :(");
            e.getStackTrace();
        }

    }

    static volatile HashMap<String, ReplicaBlockingStub> nameToStub = new HashMap<>();
    static ReplicaBlockingStub myPredecessorStub = null;
    static ReplicaBlockingStub mySuccessorStub = null;

    static boolean awaitingStateTransfer = true;

    public static void calculateNeighborsAndUpdateStubs() {

        try {
            stubSem.acquire();

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

            Collections.sort(currentChildren);
            System.out.println(currentChildren);

            int myIndex = currentChildren.indexOf(myNodeName);
            String myPredecessorName = (myIndex == 0) ? null : currentChildren.get(myIndex - 1);
            String mySuccessorName = (myIndex == (currentChildren.size() - 1)) ? null
                    : currentChildren.get(myIndex + 1);

            if (myPredecessorName == null) {
                System.out.println("I am the head now!");
                awaitingStateTransfer = false;
                myPredecessorStub = null;
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
                ReplicaBlockingStub newPredecessorStub = nameToStub.get(myPredecessorName);
                if (newPredecessorStub != myPredecessorStub) {
                    awaitingStateTransfer = true;
                    // i have a new predecessor and should have a state transfer
                }
                myPredecessorStub = newPredecessorStub;
            }

            if (mySuccessorName == null) {
                System.out.println("I am the tail now!");
                mySuccessorStub = null;
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
                if (mySuccessorStub != newSuccessorStub) {
                    // sending state transfer
                    System.out.println("Sending state transfer...");

                    StateTransferRequest req = StateTransferRequest.newBuilder().putAllState(KVStore)
                            .setXid(globalTxIDPending).addAllSent(sentList).build();
                    StateTransferResponse res = newSuccessorStub.stateTransfer(req);
                    while (res == null) {
                        try {
                            res = newSuccessorStub.stateTransfer(req);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }
                mySuccessorStub = newSuccessorStub;
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

    static class Head extends HeadChainReplicaImplBase {
        @Override
        public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
            System.out.println("\nGot increment request");
            try {
                idlingSem.acquire();
                stubSem.acquire();
                headResponseObserver = responseObserver; // used to send incresponse back when ack is returned

                if (myPredecessorStub != null) {
                    System.out.println("\nNot the head");
                    HeadResponse notHead = HeadResponse.newBuilder().setRc(1).build();
                    responseObserver.onNext(notHead);
                    responseObserver.onCompleted();
                } else {
                    awaitingStateTransfer = false;

                    if (!KVStore.containsKey(request.getKey())) {
                        KVStore.put(request.getKey(), 0);
                    }
                    int newValue = KVStore.get(request.getKey()) + request.getIncValue();
                    KVStore.put(request.getKey(), newValue);
                    System.out.println("Put value " + newValue + " for key: " + request.getKey());
                    globalTxIDPending++;

                    if (mySuccessorStub != null) { // yes successor
                        UpdateRequest req = UpdateRequest.newBuilder().setKey(request.getKey()).setNewValue(newValue)
                                .setXid(globalTxIDPending).build();

                        UpdateResponse res = mySuccessorStub.update(req);
                        while (mySuccessorStub != null && res == null) {
                            System.out.println("\nTrying to forward update as head to successor");
                            stubSem.release();
                            Thread.sleep(500);
                            stubSem.acquire();
                            res = mySuccessorStub.update(req);
                        }
                        if (mySuccessorStub != null) {
                            sentList.add(req);

                        } else {
                            globalTxIDAcked = globalTxIDPending; // treat request as acked
                            System.out.println("\nno successor, sending back response immediately :(");
                            HeadResponse actuallyHead = HeadResponse.newBuilder().setRc(0).build();
                            responseObserver.onNext(actuallyHead);
                            responseObserver.onCompleted();
                        }
                    } else {
                        globalTxIDAcked = globalTxIDPending; // treat request as acked
                        System.out.println("\nno successor, sending back response immediately :(");
                        HeadResponse actuallyHead = HeadResponse.newBuilder().setRc(0).build();
                        responseObserver.onNext(actuallyHead);
                        responseObserver.onCompleted();
                    }
                }
            } catch (Exception e) {
                System.out.println("\n Error while processing an increment request");
                e.printStackTrace();
            } finally {
                stubSem.release();
                idlingSem.release();
            }

        }
    }

    static class Tail extends TailChainReplicaImplBase {
        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            System.out.println("\nGot get request");
            try {
                stubSem.acquire();
                int val = KVStore.containsKey(request.getKey()) ? KVStore.get(request.getKey()) : 0;
                GetResponse actuallyTail = GetResponse.newBuilder().setRc(mySuccessorStub == null ? 0 : 1).setValue(val)
                        .build();
                responseObserver.onNext(actuallyTail);
                responseObserver.onCompleted();
                System.out.println("\nResponding back");

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
                idlingSem.acquire();
                stubSem.acquire();
                ChainDebugResponse.Builder builder = ChainDebugResponse.newBuilder();

                builder.addAllSent(sentList);
                builder.putAllState(KVStore);
                builder.setXid(globalTxIDPending);

                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                System.out.println("Error while processing a debug request");
                e.printStackTrace();
            } finally {
                stubSem.release();
                idlingSem.release();
            }

        }

        @Override
        public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
            System.out.println("\nGot exit request");
            responseObserver.onNext(ExitResponse.newBuilder().build());
            responseObserver.onCompleted();

            idlingSem.release();
            stubSem.release();
            System.exit(0);
        }
    }

    static class Replica extends ReplicaImplBase {
        @Override
        public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {

            System.out.println("\nGot update request");
            try {
                idlingSem.acquire();
                stubSem.acquire();
                globalTxIDPending = request.getXid();
                System.out.println("\nthe id is: " + request.getXid());

                KVStore.put(request.getKey(), request.getNewValue());

                UpdateResponse res = UpdateResponse.newBuilder().setRc(0).build();
                responseObserver.onNext(res);
                responseObserver.onCompleted();

                if (mySuccessorStub == null) {
                    System.out.println("\nI am the tail");
                    System.out.println("\nSending the ack back: " + request.getXid());
                    globalTxIDAcked = request.getXid();

                    AckRequest req = AckRequest.newBuilder().setXid(request.getXid()).build();
                    AckResponse aRes = myPredecessorStub.ack(req);
                    while (myPredecessorStub != null && aRes == null) {
                        System.out.println("Trying to send ack back to pred");
                        stubSem.release();
                        Thread.sleep(500);
                        stubSem.acquire();
                        aRes = myPredecessorStub.ack(req);
                    }

                } else {
                    System.out.println("\nForwarding to successor");
                    UpdateResponse uRes = mySuccessorStub.update(request);
                    while (mySuccessorStub != null && uRes == null) {
                        System.out.println("\nTrying to forward update as head to successor");
                        stubSem.release();
                        Thread.sleep(500);
                        stubSem.acquire();
                        res = mySuccessorStub.update(request);
                    }
                    if (mySuccessorStub != null) {
                        sentList.add(request);
                    } else {
                        System.out.println("\nI am the tail");
                        System.out.println("\nSending the ack back: " + request.getXid());
                        globalTxIDAcked = request.getXid();

                        AckRequest req = AckRequest.newBuilder().setXid(request.getXid()).build();
                        AckResponse aRes = myPredecessorStub.ack(req);
                        while (myPredecessorStub != null && aRes == null) {
                            System.out.println("Trying to send ack back to pred");
                            stubSem.release();
                            Thread.sleep(500);
                            stubSem.acquire();
                            aRes = myPredecessorStub.ack(req);
                        }
                    }

                }

            } catch (Exception e) {
                System.out.println("\nError while processing an update request");
                e.printStackTrace();
            } finally {
                stubSem.release();
                idlingSem.release();
            }

        }

        @Override
        public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
            System.out.println("\nGot ack request");
            try {
                // idlingSem.acquire();
                stubSem.acquire();

                globalTxIDAcked = request.getXid();

                responseObserver.onNext(AckResponse.newBuilder().build());
                responseObserver.onCompleted();

                if (myPredecessorStub == null) {
                    System.out.println("\nI am the head, sending back head response");
                    HeadResponse successfulAck = HeadResponse.newBuilder().setRc(0).build();
                    headResponseObserver.onNext(successfulAck);
                    headResponseObserver.onCompleted();

                } else {
                    System.out.println("\nForwarding ack to predecessor with id: " + request.getXid());
                    AckResponse aRes = myPredecessorStub.ack(request);
                    while (myPredecessorStub != null && aRes == null) {
                        System.out.println("Trying to send ack back to pred");
                        stubSem.release();
                        Thread.sleep(500);
                        stubSem.acquire();
                        aRes = myPredecessorStub.ack(request);
                    }
                }

                UpdateRequest reqToDelete = null;
                for (UpdateRequest r : sentList) {
                    if (r.getXid() == request.getXid()) {
                        reqToDelete = r;
                        break;
                    }
                }
                if (reqToDelete != null) {
                    sentList.remove(reqToDelete);
                }

            } catch (Exception e) {
                System.out.println("\nError while processing an ack request");
                e.printStackTrace();
            } finally {
                stubSem.release();
                // idlingSem.release();
            }
        }

        @Override
        public void stateTransfer(StateTransferRequest request,
                StreamObserver<StateTransferResponse> responseObserver) {

            System.out.println("\nGot StateTransfer request");
            try {
                idlingSem.acquire();
                stubSem.acquire();
                if (!awaitingStateTransfer) {
                    System.out.println("\nNo need for a state transfer");
                    StateTransferResponse noNeed = StateTransferResponse.newBuilder().setRc(1).build();

                    responseObserver.onNext(noNeed);
                    responseObserver.onCompleted();
                } else {
                    awaitingStateTransfer = false;
                    StateTransferResponse thanks = StateTransferResponse.newBuilder().setRc(0).build();
                    responseObserver.onNext(thanks);
                    responseObserver.onCompleted();

                    if (myPredecessorStub == null) {
                        stubSem.release();
                        calculateNeighborsAndUpdateStubs();
                        stubSem.acquire();
                    }

                    while (myPredecessorStub == null) {
                        stubSem.release();
                        calculateNeighborsAndUpdateStubs();
                        stubSem.acquire();
                        Thread.sleep(500);
                    }

                    System.out.println("\nMerging state transfers with memory");

                    Map<String, Integer> recievedKVStore = request.getStateMap();
                    for (String key : recievedKVStore.keySet()) {
                        System.out.println("For " + key + ", merging KVStore");
                        KVStore.put(key, recievedKVStore.get(key));
                    }

                    List<UpdateRequest> recievedSentList = request.getSentList();
                    for (UpdateRequest newUReq : recievedSentList) {
                        if (newUReq.getXid() < globalTxIDAcked) {
                            // request has already been processed, just resend ack
                            AckRequest resendingOld = AckRequest.newBuilder().setXid(newUReq.getXid()).build();
                            if (myPredecessorStub != null) {
                                System.out.println("\nResending ack for tx id: " + newUReq.getXid());

                                AckResponse aRes = myPredecessorStub.ack(resendingOld);
                                while (myPredecessorStub != null && aRes == null) {
                                    System.out.println("Trying to send ack back to pred");
                                    stubSem.release();
                                    Thread.sleep(500);
                                    stubSem.acquire();
                                    aRes = myPredecessorStub.ack(resendingOld);
                                }

                            } else {
                                System.out.println("\nOops I have no predecessor?!");
                            }
                            continue;
                        }

                        // potentially a new request
                        boolean isNewReq = true;
                        for (UpdateRequest sReq : sentList) {
                            if (sReq.getXid() == newUReq.getXid()) {
                                isNewReq = false;
                                break;
                            }
                        }

                        if (isNewReq) {
                            // not adding to KVStore, assuming thats already up to date
                            if (mySuccessorStub != null) {
                                System.out.println("\nForwarding request with tx id:" + newUReq.getXid());
                                globalTxIDPending = Integer.max(globalTxIDPending, newUReq.getXid());
                                mySuccessorStub.update(newUReq);
                                sentList.add(newUReq);

                                UpdateResponse uRes = mySuccessorStub.update(newUReq);
                                while (mySuccessorStub != null && uRes == null) {
                                    System.out.println("\nTrying to forward update as head to successor");
                                    stubSem.release();
                                    Thread.sleep(500);
                                    stubSem.acquire();
                                    uRes = mySuccessorStub.update(newUReq);
                                }

                            } else {
                                // assuming im the tail now, no point adding to sentList
                                System.out.println(
                                        "\nNo successor to forward request to, assuming I am tail and sending ack back instead");
                                globalTxIDAcked = Integer.max(globalTxIDAcked, newUReq.getXid());
                                AckRequest newAck = AckRequest.newBuilder().setXid(newUReq.getXid()).build();
                                AckResponse aRes = myPredecessorStub.ack(newAck);
                                while (myPredecessorStub != null && aRes == null) {
                                    System.out.println("Trying to send ack back to pred");
                                    stubSem.release();
                                    Thread.sleep(500);
                                    stubSem.acquire();
                                    aRes = myPredecessorStub.ack(newAck);
                                }
                            }
                        }
                    }
                    // ack should autoupdate
                    globalTxIDPending = Integer.max(globalTxIDPending, request.getXid());

                }

            } catch (Exception e) {
                System.out.println("\nError while processing a StateTransfer request");
                e.printStackTrace();
            } finally {
                stubSem.release();
                idlingSem.release();
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