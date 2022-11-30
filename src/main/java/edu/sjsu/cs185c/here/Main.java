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
import org.apache.zookeeper.data.Stat;

import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaBlockingStub;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Main {

    static final boolean debug = true;
    static HashMap<String, ReplicaBlockingStub> portToStub = new HashMap<>();

    // One time usage static references
    static Cli execObj;
    static ZooKeeper zk;
    static Semaphore mainThreadSem;
    static String myNodeName;

    //Static references that could change
    static String myPredecessor;
    static String mySuccessor;

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
            mainThreadSem = new Semaphore(1);
            mainThreadSem.acquire();

            startZookeeper();
            mainThreadSem.acquire(); // main thread spinning
            return 0;
        }
    }

    public static void startZookeeper() { // starting point of program

        try {
            zk = new ZooKeeper(execObj.serverList, 10000, (e) -> {
                if (e.getState() == Watcher.Event.KeeperState.Expired){
                    System.exit(0);
                }
            });

            
            try {
                zk.create(execObj.controlPath + "/replica-", (execObj.hostPort + "\nujjwal").getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);                
                MembershipWatcher memWatch = new MembershipWatcher();
                memWatch.process(null);

            } catch (KeeperException | InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

        } catch (IOException e) {
            System.out.println("Zookeeper Not Found");
            e.getStackTrace();
        }

    }


    static class MembershipWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            System.out.println("==========");
            try {
                List<String> currentChildren = zk.getChildren(execObj.controlPath, new MembershipWatcher());

                for (String child : currentChildren) {
                    Stat current = null;
                    byte[] dataBytes = zk.getData(execObj.controlPath + "/" + child, null, current);
                    if(dataBytes == null){ 
                        System.out.println("skipping " + child);
                        continue;
                    }
                    String data = new String(dataBytes);
                    String[] splitted = data.split("\n");
                    if (!portToStub.containsKey(splitted[0])) {
                        System.out.println("Found " + splitted[1] + "@" + splitted[0]);

                        if (!splitted[0].equals(execObj.hostPort)) {
                            ManagedChannel newChannel = ManagedChannelBuilder.forTarget(splitted[0])
                                    .usePlaintext()
                                    .build();
                            ReplicaBlockingStub newStub = ReplicaGrpc.newBlockingStub(newChannel);
                            portToStub.put(splitted[0], newStub);
                        } else {
                            System.out.println(child + " is me");
                            myNodeName = child;
                        }
                    }
                }

                Collections.sort(currentChildren);
                
                int myIndex = currentChildren.indexOf(myNodeName);
                myPredecessor = (myIndex == 0) ? "null" : currentChildren.get(myIndex - 1);
                mySuccessor = (myIndex == (currentChildren.size() - 1)) ? "null" : currentChildren.get(myIndex + 1);

                System.out.println(myPredecessor + " is my predecessor");
                System.out.println(mySuccessor + " is my successor");

            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("==========");
        }
        
    }

    public static void main(String[] args) {
        execObj = new Cli();
        System.exit(new CommandLine(execObj).execute(args));
    }
}
