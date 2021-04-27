import com.alibaba.fastjson.JSON;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 开源客户端ZkClient
 */
public class ZkClientTest {

    public static ZkClient createSession() {
        ZkClient zc = new ZkClient("localhost:2181", 10000, 10000, new SerializableSerializer());
        System.out.println("conneted ok!");
        return zc;
    }

    public static final String path = "/jike5";

    public static final ZkClient zc = createSession();

    /**
     * 数据监听
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 除子节点变化外，节点本身创建和删除也会收到通知
        zc.subscribeChildChanges(path, new IZkChildListener() {
            @Override
            public void handleChildChange(String s, List<String> list) throws Exception {
                System.out.println(String.format("handleChildChange:%s,%s", s, JSON.toJSONString(list)));
            }
        });

        // 监听数据变化
        zc.subscribeDataChanges(path, new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {
                System.out.println(String.format("handleDataChange:%s,%s", s, JSON.toJSONString(o)));
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                System.out.println(String.format("handleDataDeleted:%s", s));
            }
        });

        new CountDownLatch(1).await();
    }

    /**
     * 增删改查
     */
    public static class Curd {
        public static void main(String[] args) throws Exception {
            System.out.println("conneted ok!");

            //增
            String createPath = zc.create(path, "test", CreateMode.EPHEMERAL);


            //查
            String readData = zc.readData(path, new Stat());
            List<String> cList = zc.getChildren(path);
            boolean e = zc.exists(path);

            //改,重载方法有  expectedVersion
            zc.writeData(path, "test2");

            //删
//            boolean e1 = zc.delete(path);
//            boolean e2 = zc.deleteRecursive(path);
        }
    }


}
