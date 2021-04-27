import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

/**
 * <dependency>
 * <groupId>org.apache.curator</groupId>
 * <artifactId>curator-framework</artifactId>
 * <version>2.8.0</version>
 * </dependency>
 *
 * <dependency>
 * <groupId>org.apache.curator</groupId>
 * <artifactId>curator-recipes</artifactId>
 * <version>2.8.0</version>
 * </dependency>
 */
public class CuratorTest {
    private static CuratorFramework client = null;

    static {
        //RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        //RetryPolicy retryPolicy = new RetryNTimes(5, 1000);
        RetryPolicy retryPolicy = new RetryUntilElapsed(5000, 1000);
//        CuratorFramework client = CuratorFrameworkFactory
//                .newClient("localhost:2181",5000,5000, retryPolicy);
        client = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        client.start();
    }

    public static final String path = "/jike6";


    /**
     * 数据监听
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 除子节点变化外，节点本身创建和删除也会收到通知
        final NodeCache cache = new NodeCache(client, path);
        cache.start();
        cache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                byte[] ret = cache.getCurrentData().getData();
                System.out.println(String.format("nodeChanged:%s", new String(ret)));

            }
        });


        // 监听子节点
        final PathChildrenCache pathChildrenCache = new PathChildrenCache(client, path, true);
        pathChildrenCache.start();
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                    throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        System.out.println("CHILD_ADDED:" + event.getData());
                        break;
                    case CHILD_UPDATED:
                        System.out.println("CHILD_UPDATED:" + event.getData());
                        break;
                    case CHILD_REMOVED:
                        System.out.println("CHILD_REMOVED:" + event.getData());
                        break;
                    default:
                        break;
                }
            }
        });

        new CountDownLatch(1).await();
    }

    /**
     * 增删改查
     */
    public static class Curd {
        public static void main(String[] args) throws Exception {
            //增
            String createPath = client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path, "test".getBytes());
            System.out.println(createPath);

            //查
            byte[] ret = client.getData().storingStatIn(new Stat()).forPath(path);
            System.out.println(new String(ret));
            List<String> cList = client.getChildren().forPath(path);

            client.checkExists().inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework arg0, CuratorEvent arg1)
                        throws Exception {
                    Stat stat = arg1.getStat();
                    System.out.println(stat);
                    System.out.println(arg1.getContext());
                }
            }, "123", Executors.newFixedThreadPool(5)).forPath(path);

            //改,重载方法有  expectedVersion
            Stat stat = new Stat();
            client.getData().storingStatIn(stat).forPath(path);
            client.setData().withVersion(stat.getVersion()).forPath(path, "test".getBytes());

            //删
            client.delete().guaranteed().deletingChildrenIfNeeded().withVersion(-1).forPath(path);

            //aclcreate
            ACL aclIp = new ACL(ZooDefs.Perms.READ, new Id("ip", "localhost"));
            ACL aclDigest = new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.WRITE, new Id("digest", DigestAuthenticationProvider.generateDigest("jike:123456")));
            ArrayList<ACL> acls = new ArrayList<ACL>();
            acls.add(aclDigest);
            acls.add(aclIp);

            String path = client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(acls)
                    .forPath("/jike/3", "123".getBytes());

            System.out.println(path);
        }
    }


}
