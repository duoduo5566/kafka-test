package com.kafkatest.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created by chenzs on 2016/11/6.
 *
 */
public class ImpartialMode {

    private static final String ZK_HOSTS = "192.168.56.5:2181,192.168.56.6:2181,192.168.56.7:2181";
    private static final String ROOT_PATH = "/chroot";
    private static final String LEADER_PATH = "/chroot/leader";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        // 建立zookeeper连接，创建zk对象
        final ZooKeeper zk = new ZooKeeper(
                ZK_HOSTS,
                6000,
                (watchedEvent) -> System.out.println("stat:" + watchedEvent.getState())
        );

        // 创建父节点
        createPersistParentNode(zk, ROOT_PATH);
        String currentPath = createEphemeralNode(zk, LEADER_PATH);
        String returnPath = getListenerNode(zk, ROOT_PATH, currentPath);

        // 如果当前节点不是leader，则对当前节点的前一个节点的设置delete watcher。
        if (!currentPath.equals(returnPath)){
            zk.exists(returnPath, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    switch (watchedEvent.getType()){
                        case NodeDeleted:
                            try {
                                leaderElection(zk, ROOT_PATH);
                            } catch (KeeperException | InterruptedException e) {
                                e.printStackTrace();
                            }
                    }
                }
            });
        }

        Thread.sleep(300000);
        zk.close();
    }

    /**
     * 创建leader election 父节点
     *
     * @param zk       zk对象
     * @param rootPath 父节点路径
     */
    private static void createPersistParentNode(final ZooKeeper zk,
                                                final String rootPath)
            throws KeeperException, InterruptedException {

        // 判断是否存在父节点
        Stat stat = zk.exists(rootPath, false);

        // 如果不存在，创建父节点
        if (stat == null) {
            zk.create(rootPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("parent node [" + rootPath + "] create success.");
        } else {
            System.out.println("parent node [" + rootPath + "] is exists, skip create.");
        }
    }

    /***
     * 创建临时的worker节点
     * @param zk
     * @param leaderPath
     * @return
     */
    private static String createEphemeralNode(final ZooKeeper zk,
                                           final String leaderPath)
            throws KeeperException, InterruptedException {
        String leaderNode = zk.create(
                leaderPath,
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL
        );

        System.out.println("leader node [" + leaderNode + "] is created success.");

        return leaderNode;
    }

    /***
     * 获取Watcher的节点
     * @param zk          zk对象
     * @param rootPath    根路径
     * @param currentPath 当前节点
     * @return            需要Watcher的节点
     */
    private static String getListenerNode(ZooKeeper zk,
                                         String rootPath,
                                         String currentPath
                                         )
            throws KeeperException, InterruptedException {

        System.out.print("i am the node [" + currentPath + "],");

        List<String> zkChildrenList = leaderElection(zk, rootPath);

        String leaderPath = zkChildrenList.get(0);
        String returnPath = currentPath.substring(8);

        if (!returnPath.equals(leaderPath)){
            int idx = zkChildrenList.indexOf(returnPath);
            returnPath = zkChildrenList.get(idx -1);
        }

        return rootPath + "/" + returnPath;
    }

    /***
     *
     * @param zk       zk对象
     * @param rootPath 根路径
     * @return         获取所有节点
     */
    private static List<String> leaderElection(ZooKeeper zk,
                                         String rootPath
                                         )
            throws KeeperException, InterruptedException {
        List<String> zkChildrenList=  zk.getChildren(
                rootPath,
                watchedEvent -> System.out.println(watchedEvent.getPath() + ":" + watchedEvent.getState())
        );

        Collections.sort(zkChildrenList);
        System.out.println("the leader is [" + zkChildrenList.get(0) + "].");
        return zkChildrenList;
    }
}