package edu.sjsu.cs185c.here;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.apache.zookeeper.ZooKeeper;

import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaBlockingStub;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;

public class Main {

    static final boolean debug = true;

    // One time usage static references
    static Cli execObj;
    static ZooKeeper zk;
    static Semaphore mainThreadSem;

    // Static references that could change
    static Semaphore idlingSem;

    @Command(name = "zoolunchleader", mixinStandardHelpOptions = true, description = "register attendance for class.")
    static class Cli implements Callable<Integer> {
        @Parameters(index = "0", description = "zookeeper_server_list")
        String serverList;

        @Parameters(index = "1", description = "control_path")
        String controlPath;

        @Parameters(index = "2", description = "host_port")
        String hostPort;

        @Override
        public Integer call() throws Exception {

            // Initial threadsafe setup
            mainThreadSem = new Semaphore(1);
            idlingSem = new Semaphore(1);

            mainThreadSem.acquire();
            idlingSem.acquire();

            startZookeeper();
            mainThreadSem.acquire(); // main thread spinning

            return 0;
        }
    }

    public static void exit() {
        idlingSem.release();
        mainThreadSem.release();
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
                    actualPath = zk.create(execObj.controlPath + "/replica-",
                            (execObj.hostPort + "\nujjwal").getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                } catch (KeeperException | InterruptedException e) {
                    System.out.println("\nCannot create my own node :(");
                    e.printStackTrace();
                }
            }

            String[] splittedPath = actualPath.split("\\/");

            String myNodeName = splittedPath[splittedPath.length - 1];
            System.out.println("My name is " + myNodeName);
            calculateNeighborsAndUpdateStubs(myNodeName);

        } catch (IOException e) {
            System.out.println("\nZookeeper Not Found :(");
            e.getStackTrace();
        }

    }

    static HashMap<String, ReplicaBlockingStub> nameToStub = new HashMap<>();
    static ReplicaBlockingStub myPredecessorStub = null;
    static ReplicaBlockingStub mySuccessorStub = null;

    public static void calculateNeighborsAndUpdateStubs(String myNodeName) {

        System.out.println("==========Fetching children==========");
        System.out.println("I am " + myNodeName);

        List<String> currentChildren = null;
        while (currentChildren == null) {
            try {
                currentChildren = zk.getChildren(execObj.controlPath, (e) -> {
                    calculateNeighborsAndUpdateStubs(myNodeName);
                });
            } catch (KeeperException | InterruptedException e) {
                System.out.println("\nCannot fetch children :(");
                e.printStackTrace();
            }
        }

        System.out.println(currentChildren);
        Collections.sort(currentChildren);

        int myIndex = currentChildren.indexOf(myNodeName);
        String myPredecessorName = (myIndex == 0) ? null : currentChildren.get(myIndex - 1);
        String mySuccessorName = (myIndex == (currentChildren.size() - 1)) ? null : currentChildren.get(myIndex + 1);

        try {
            idlingSem.acquire();

            if ((myPredecessorName != null) && (!nameToStub.containsKey(myPredecessorName))) {
                nameToStub.put(myPredecessorName, createStub(myPredecessorName));
            }

            if ((mySuccessorName != null) && (!nameToStub.containsKey(mySuccessorName))) {
                nameToStub.put(mySuccessorName, createStub(mySuccessorName));
            }

            if (myPredecessorName == null) {
                System.out.println("I am the head now!");
                myPredecessorStub = null;
            } else {
                System.out.println(myPredecessorName + " is my predecessor!");
                myPredecessorStub = nameToStub.get(myPredecessorName);
            }

            if (mySuccessorName == null) {
                System.out.println("I am the tail now!");
                mySuccessorStub = null;
            } else {
                System.out.println(mySuccessorName + " is my successor!");
                mySuccessorStub = nameToStub.get(mySuccessorName);
            }

        } catch (KeeperException | InterruptedException | NullPointerException e) {
            System.out.println("\nError while changing successor and/or predecessor stubs :(");
            e.printStackTrace();
        } finally {
            idlingSem.release();
            System.out.println("=====================================");
        }

    }

    public static ReplicaBlockingStub createStub(String nodeName)
            throws KeeperException, InterruptedException, NullPointerException {

        byte[] dataBytes = zk.getData(execObj.controlPath + "/" + nodeName, null, null);
        String data = new String(dataBytes);
        String[] splitted = data.split("\n");
        System.out.println(nodeName + " is actually " + splitted[1] + "@" + splitted[0]);

        ManagedChannel newChannel = ManagedChannelBuilder.forTarget(splitted[0])
                .usePlaintext()
                .build();
        return ReplicaGrpc.newBlockingStub(newChannel);

    }

    public static void main(String[] args) {
        execObj = new Cli();
        System.exit(new CommandLine(execObj).execute(args));
    }
}
