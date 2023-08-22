package amrk7.exp.flink.functions.source;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.ACLProvider;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.flink.shaded.curator4.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.WatchedEvent;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.Watcher;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZkRegistry {

    private CuratorFramework zooKeeperClient;
    private boolean deregisteredWithZooKeeper;
    PersistentEphemeralNode znode = null;

    private String rootNamespace = "test";

    private void setDeregisteredWithZooKeeper(boolean deregisteredWithZooKeeper) {
        this.deregisteredWithZooKeeper = deregisteredWithZooKeeper;
    }

    public ZkRegistry(String zooKeeperEnsemble,
                      ACLProvider aclProvider,
                      int sessionTimeout, int baseSleepTime, int maxRetries) {
        zooKeeperClient =
                CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
                        .sessionTimeoutMs(sessionTimeout)
                        .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries)).build();
        zooKeeperClient.start();
    }

    public List<String> discoverInstances() throws Exception {
        return zooKeeperClient
                .getChildren()
                .forPath("/" + rootNamespace + "/servers").stream().collect(Collectors.toList());
    }

    // copied from Hive
    public void addServerInstanceToAZkNamespace(
            String instanceURI
    ) throws Exception {
        String znodePath = null;
        // Create the parent znodes recursively; ignore if the parent already exists.
        try {
            zooKeeperClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                    .forPath("/" + rootNamespace);
            System.out.println("Created the root name space: " + rootNamespace + " on ZooKeeper for HiveServer2");
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                System.out.println("Unable to create namespace for the Flink listener: " + rootNamespace + " on ZooKeeper");
                e.printStackTrace();
                throw e;
            }
        }
        // Create a znode under the rootNamespace parent for this instance of the server
        // Znode name: serverUri=host:port;version=versionInfo;sequence=sequenceNumber
        try {
            String pathPrefix =
                    "/" + rootNamespace + "/servers/" + instanceURI;
            final String znodeData = "";
            byte[] znodeDataUTF8 = znodeData.getBytes(Charset.forName("UTF-8"));
            znode =
                    new PersistentEphemeralNode(zooKeeperClient,
                            PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, pathPrefix, znodeDataUTF8);
            znode.start();
            // We'll wait for 120s for node creation
            long znodeCreationTimeout = 120;
            if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
                throw new Exception("Max znode creation wait time: " + znodeCreationTimeout + "s exhausted");
            }
            setDeregisteredWithZooKeeper(false);
            znodePath = znode.getActualPath();
            // Set a watch on the znode
            if (zooKeeperClient.checkExists().usingWatcher(new DeRegisterWatcher()).forPath(znodePath) == null) {
                // No node exists, throw exception
                throw new Exception("Unable to create znode for this HiveServer2 instance on ZooKeeper.");
            }
            System.out.println("Created a znode on ZooKeeper for HiveServer2 uri: " + instanceURI);
        } catch (Exception e) {
            System.out.println("Unable to create a znode for this server instance");
            e.printStackTrace();
            if (znode != null) {
                znode.close();
            }
            throw (e);
        }
    }

    private class DeRegisterWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
                System.out.println("Node is deleted, terminating this split");
            }
        }
    }
}
