package edu.sjsu.cs185c.here;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.IOException;
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
    static final String myPort = "1111";
    static String grpcHostPort = debug ? "localhost:" + myPort : "172.27.17.13:" + myPort;
    static List<String> currentChildren;

    static HashMap<String, ReplicaBlockingStub> portToStub = new HashMap<>();

    // One time usage static references
    static Cli execObj;
    static ZooKeeper zk;
    static Semaphore mainThreadSem;

    @Command(name = "zoolunchleader", mixinStandardHelpOptions = true, description = "register attendance for class.")
    static class Cli implements Callable<Integer> {
        @Parameters(index = "0", description = "zookeeper_server_list")
        String serverList;

        @Parameters(index = "1", description = "control_path")
        String controlPath;

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
            zk = new ZooKeeper(execObj.serverList, 10000, new ConnectedWatcher());
        } catch (IOException e) {
            System.out.println("Zookeeper Not Found");
            e.getStackTrace();
        }

    }

    static class ConnectedWatcher implements Watcher { // executes when connection is successful

        @Override
        public void process(WatchedEvent event) {

            System.out.println("==============");
            System.out.println("Connected!");
            System.out.println("==============");

            try {
                zk.create(execObj.controlPath + "/replica-", (grpcHostPort + "\nujjwal").getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                zk.create(execObj.controlPath + "/replica-", (grpcHostPort + "\nujjwal").getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                currentChildren = zk.getChildren(execObj.controlPath, new MembershipWatcher());
                System.out.println(currentChildren);

            } catch (KeeperException | InterruptedException e1) {
                e1.printStackTrace();
            }

        }
    }

    static class MembershipWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {

            try {
                zk.exists(execObj.controlPath, new MembershipWatcher());
                for (String child : currentChildren) {
                    Stat current = null;
                    byte[] dataBytes = zk.getData(execObj.controlPath + "/" + child, null, current);
                    String data = new String(dataBytes);
                    String[] splitted = data.split("\n");
                    if (!portToStub.containsKey(splitted[0])) {
                        System.out.println("Found " + splitted[1] + "@" + splitted[0]);

                        if (splitted[0] != grpcHostPort) {
                            ManagedChannel newChannel = ManagedChannelBuilder.forTarget(splitted[0])
                                    .usePlaintext()
                                    .build();
                            ReplicaBlockingStub newStub = ReplicaGrpc.newBlockingStub(newChannel);
                            portToStub.put(splitted[0], newStub);
                        } else {
                            System.out.println(child + " is me");
                        }
                    }
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) {
        execObj = new Cli();
        System.exit(new CommandLine(execObj).execute(args));
    }
}
