package com.qunar.zkclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qunar.zkclient.exception.ZkException;
import com.qunar.zkclient.listener.NodeChildrenListener;
import com.qunar.zkclient.listener.NodeDataListener;
import com.qunar.zkclient.util.ZkLogger;

/**
 * 客户端使用的类
 * 
 * 
 * TODO:支持ACL
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class ZkClient {

    private static final Logger logger = LoggerFactory.getLogger(ZkLogger.class);

    // zk session timeout
    private static final int SESSION_TIMEOUT = 5000;

    /** children listener */
    private Map<String, List<NodeChildrenListener>> childrenListeners;

    /** children listener read-write lock */
    private ReentrantReadWriteLock childrenRWLock;

    /** node listener */
    private Map<String, List<NodeDataListener>> nodeListeners;

    /** node listener read-write lock */
    private ReentrantReadWriteLock nodeRWLock;

    /** zk resource start */

    /** watcher and zk lock */
    private ZkWatcher watcher;

    private String zkAddress;

    private ZooKeeper zk;

    private NotifyTask notifyTask;

    private Thread notifyThread;

    /** zk resource done */

    private static final Map<String, ZkClient> map = new HashMap<String, ZkClient>();

    public static ZkClient getInstance(String zkAddress) {
        ZkClient client = null;
        synchronized (ZkClient.class) {
            client = map.get(zkAddress);
            if (client != null) {
                return client;
            } else {
                try {
                    client = new ZkClient(zkAddress);
                    map.put(zkAddress, client);
                } catch (Exception e) {
                    logger.error("failed to new ZkClient zkAddress: " + zkAddress, e);
                }
            }
        }
        return client;
    }

    private ZkClient(String zkAddr) {
        this.zkAddress = zkAddr;
        childrenListeners = new HashMap<String, List<NodeChildrenListener>>();
        childrenRWLock = new ReentrantReadWriteLock();
        nodeListeners = new HashMap<String, List<NodeDataListener>>();
        nodeRWLock = new ReentrantReadWriteLock();
        initialize();
    }

    private void initialize() {
        watcher = new ZkWatcher(true);
        synchronized (watcher) {
            try {
                zk = new ZooKeeper(zkAddress, SESSION_TIMEOUT, watcher);
                watcher.wait();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        notifyTask = new NotifyTask();
        notifyTask.setZkClient(this);
        notifyThread = new Thread(notifyTask);
        notifyThread.setName("NotifyTaskThread");
        notifyThread.setDaemon(true);
        notifyThread.start();
    }

    private boolean reopenZkConnection() {
        synchronized (watcher) {
            try {
                watcher.setFirst(false);
                zk = new ZooKeeper(zkAddress, SESSION_TIMEOUT, watcher);
                watcher.wait();
                return true;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return false;
    }

    public void close() {
        closeNotifyThread();
        closeZkConnection();
    }

    private void closeNotifyThread() {
        // interrupt notifyThread
        if (notifyThread != null) {
            try {
                logger.debug("notifyThread.isInterrupted(): " + notifyThread.isInterrupted());
                notifyThread.interrupt();
            } catch (Exception e) {
                logger.error("ZkClient.close()", e);
            }
        }
    }

    private void closeZkConnection() {
        // release zk resource
        synchronized (watcher) {
            if (zk != null) {
                try {
                    logger.debug("close zk!");
                    zk.close();
                } catch (Exception e) {
                    logger.error("ZkClient.close()", e);
                }
            }
        }
    }

    /**
     * Add NodeDataListener
     * 
     * @param listener
     */
    public void addNodeDataListener(NodeDataListener listener) {
        if (listener == null) {
            return;
        }
        try {
            nodeRWLock.writeLock().lock();
            String nodePath = listener.getNodePath();
            List<NodeDataListener> list = nodeListeners.get(nodePath);
            if (list != null) {
                list.add(listener);
            } else {
                list = new ArrayList<NodeDataListener>();
                list.add(listener);
                nodeListeners.put(nodePath, list);
            }
        } finally {
            nodeRWLock.writeLock().unlock();
        }
    }

    /**
     * Add NodeChildrenListener
     * 
     * @param listener
     */
    public void addNodeChildrenListener(NodeChildrenListener listener) {
        if (listener == null) {
            return;
        }
        try {
            childrenRWLock.writeLock().lock();
            String nodePath = listener.getNodePath();
            List<NodeChildrenListener> list = childrenListeners.get(nodePath);
            if (list != null) {
                list.add(listener);
            } else {
                list = new ArrayList<NodeChildrenListener>();
                list.add(listener);
                childrenListeners.put(nodePath, list);
            }
        } finally {
            childrenRWLock.writeLock().unlock();
        }
    }

    /**
     * 
     * @param nodePath
     */
    boolean updateNode(String nodePath) {
        String newValue = null;
        try {
            newValue = getData(nodePath, true);
        } catch (ZkException e) {
            logger.error(e.getMessage(), e);
        }
        if (newValue == null) {
            logger.debug("ZkClient.updatNode() path: " + nodePath + ", newValue == null");
            return true;
        }
        boolean res = true;
        try {
            nodeRWLock.readLock().lock();
            List<NodeDataListener> listeners = nodeListeners.get(nodePath);
            if (listeners != null) {
                for (NodeDataListener listener : listeners) {
                    res = listener.update(newValue);
                    // TODO: Reconsider not to break;
                    if (!res) {
                        break;
                    }
                }
            }
        } finally {
            nodeRWLock.readLock().unlock();
        }
        return res;
    }

    /**
     * 
     * @param nodePath
     */
    public boolean deleteNode(String nodePath) {
        boolean res = true;
        try {
            nodeRWLock.readLock().lock();
            List<NodeDataListener> listeners = nodeListeners.get(nodePath);
            if (listeners != null) {
                for (NodeDataListener listener : listeners) {
                    res = listener.delete();
                    // TODO: Reconsider not to break;
                    if (!res) {
                        break;
                    }
                }
            }
        } finally {
            nodeRWLock.readLock().unlock();
        }
        return res;
    }

    /**
     * 
     * @param parentNodePath
     */
    boolean updateChildren(String parentNodePath) {
        List<String> newChildren = null;
        try {
            newChildren = getChildren(parentNodePath, true);
        } catch (ZkException e) {
            logger.error("ZkClient.updateChildren() parentNodePath:" + parentNodePath, e);
        }
        if (newChildren == null) {
            logger.debug("ZkClient.updateChildren() newChildren == null, parentNodePath: "
                    + parentNodePath);
            return true;
        }
        boolean res = true;
        try {
            childrenRWLock.readLock().lock();
            List<NodeChildrenListener> listeners = childrenListeners.get(parentNodePath);
            if (listeners != null) {
                for (NodeChildrenListener listener : listeners) {
                    res = listener.update(newChildren);
                    // TODO: Reconsider not to break;
                    if (!res) {
                        break;
                    }
                }
            }
            return res;
        } finally {
            childrenRWLock.readLock().unlock();
        }
    }

    /**
     * 
     */
    void updateAllNodesAndChildren() {
        // update all NodeDataListeners
        try {
            nodeRWLock.readLock().lock();
            for (String nodePath : nodeListeners.keySet()) {
                updateNode(nodePath);
            }
        } finally {
            nodeRWLock.readLock().unlock();
        }

        // update all NodeChildrenListeners
        try {
            childrenRWLock.readLock().lock();
            for (String parentNodePath : childrenListeners.keySet()) {
                updateChildren(parentNodePath);
            }
        } finally {
            childrenRWLock.readLock().unlock();
        }
    }

    /**
     * getData
     * 
     * @param path
     * @param watch
     * @return
     * @throws ZkException
     */
    public String getData(String path, boolean watch) throws ZkException {
        if (!exist(path, false)) {
            return null;
        }
        String value = null;
        synchronized (watcher) {
            Stat stat = new Stat();
            byte[] data = null;
            try {
                data = zk.getData(path, watch, stat);
                logger.debug("stat: " + stat);
            } /*catch (ConnectionLossException e) {
                logger.error("ZkClient.getData() path: " + path, e);
                this.reopenZkConnection();
              } */
            catch (Exception e) {
                logger.error("ZkClient.getData() path: " + path, e);
                throw new ZkException(e);
            }
            if (data != null && data.length != 0) {
                value = new String(data);
            }
        }
        return value;
    }

    /**
     * exist
     * 
     * @param path
     * @param watch
     * @return
     * @throws ZkException
     */
    public boolean exist(String path, boolean watch) throws ZkException {
        synchronized (watcher) {
            try {
                Stat stat = zk.exists(path, watch);
                if (stat != null) {
                    return true;
                }
            } /*catch (ConnectionLossException e) {
                logger.error("ZkClient.exist() path: " + path, e);
                this.reopenZkConnection();
              } */
            catch (Exception e) {
                logger.error("ZkClient.exist() path: " + path, e);
                throw new ZkException(e);
            }
        }
        return false;
    }

    /**
     * getChildren
     * 
     * @param path
     * @param watch
     * @return
     * @throws ZkException
     */
    public List<String> getChildren(String path, boolean watch) throws ZkException {
        if (!exist(path, false)) {
            return null;
        }
        List<String> children = null;
        synchronized (watcher) {
            try {
                children = zk.getChildren(path, watch);
            }/* catch (ConnectionLossException e) {
                logger.error("ZkClient.getChildren() path: " + path, e);
                this.reopenZkConnection();
             } */
            catch (Exception e) {
                logger.error("ZkClient.getChildren() path: " + path, e);
                throw new ZkException(e);
            }
        }
        return children;
    }

    // TODO: deleteNode method??? let me think about it for some while!!!
    // TODO: deleteAllChildren method??? let me think about it for some while!!!

    /**
     * 
     * @param path
     * @param data
     * @return
     * @throws ZkException
     */
    public boolean createEphemeralNode(String path, String data) throws ZkException {
        if ("".equals(path)) {
            return false;
        }
        if ("".equals(data)) {
            return false;
        }
        synchronized (watcher) {
            try {
                zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                return true;
            }/* catch (ConnectionLossException e) {
                logger.error("ZkClient.createEphemeralNode() path: " + path, e);
                this.reopenZkConnection();
                throw new ZkException(e);
             } */
            catch (Exception e) {
                logger.error("ZkClient.createEphemeralNode() path: " + path + ", data: " + data, e);
                throw new ZkException(e);
            }
        }

    }

    /**
     * 
     * @param path
     * @param data
     * @return
     * @throws ZkException
     */
    public boolean createPersistentNode(String path, String data) throws ZkException {
        if ("".equals(path)) {
            return false;
        }
        if ("".equals(data)) {
            return false;
        }
        synchronized (watcher) {
            try {
                zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                return true;
            }/* catch (ConnectionLossException e) {
                logger.error("ZkClient.createPersistentNode() path: " + path, e);
                this.reopenZkConnection();
                throw new ZkException(e);
             } */
            catch (Exception e) {
                logger.error("ZkClient.createPersistentNode() path: " + path + ", data: " + data, e);
                throw new ZkException(e);
            }
        }
    }

    /**
     * 
     * @param path
     * @param data
     * @return
     * @throws ZkException
     */
    public boolean setData(String path, String data) throws ZkException {
        if ("".equals(path)) {
            return false;
        }
        if ("".equals(data)) {
            return false;
        }
        if (!exist(path, false)) {
            return false;
        }
        synchronized (watcher) {
            try {
                Stat stat = zk.setData(path, data.getBytes(), -1);
                logger.debug("ZkClient.setData() stat:" + stat);
                return true;
            }/* catch (ConnectionLossException e) {
                logger.error("ZkClient.setData() path: " + path, e);
                this.reopenZkConnection();
                throw new ZkException(e);
             } */
            catch (Exception e) {
                logger.error("ZkClient.setData() path: " + path + ", data: " + data, e);
                throw new ZkException(e);
            }
        }
    }

    /** watcher */
    private class ZkWatcher implements Watcher {

        private boolean first;

        ZkWatcher(boolean first) {
            this.first = first;
        }

        public void setFirst(boolean first) {
            this.first = first;
        }

        public void process(WatchedEvent event) {

            logger.info("event:" + event);

            EventType eventType = event.getType();
            KeeperState state = event.getState();

            String path = event.getPath();
            switch (eventType) {
                case None:
                    switch (state) {
                        case SyncConnected:
                            synchronized (this) {
                                this.notifyAll();
                            }
                            if (!first) {
                                // after last session Expired, new zk started!
                                notifyTask.addMessage(new Message("", Message.NODE_REFRESH)); //This is holy shit, and we should refresh all listeners!
                                logger.debug("new zk started!");
                            }
                            return;
                        case Disconnected:
                            break;
                        case Expired:
                            boolean exit = false;
                            while (!exit) {
                                closeZkConnection();
                                exit = reopenZkConnection();
                            }
                        default:
                            break;
                    }
                    break;
                case NodeChildrenChanged:
                    if (!path.isEmpty()) {
                        notifyTask.addMessage(new Message(path, Message.NODE_CHILDREN_CHANGED));
                        try {
                            exist(path, true);
                        } catch (ZkException e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                    break;
                case NodeDeleted:
                    if (!path.isEmpty()) {
                        notifyTask.addMessage(new Message(path, Message.NODE_DELETED));
                        try {
                            exist(path, true);
                        } catch (ZkException e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                    break;
                case NodeCreated:
                    if (!path.isEmpty()) {
                        notifyTask.addMessage(new Message(path, Message.NODE_CREATED));
                        try {
                            exist(path, true);
                        } catch (ZkException e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                    break;
                case NodeDataChanged:
                    if (!path.isEmpty()) {
                        notifyTask.addMessage(new Message(path, Message.NODE_DATA_CHANGED));
                        try {
                            exist(path, true);
                        } catch (ZkException e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }

    /*
    public static void main(String[] args) {
        BasicConfigurator.configure();
        final ZkClient zc = ZkClient.getInstance("127.0.0.1:2181");
        zc.addNodeDataListener(new NodeDataListener("/test") {

            @Override
            public boolean update(String value) {
                logger.debug("============ path: /test newvalue: " + value);
                return true;
            }

            @Override
            public boolean delete() {
                return true;
            }
        });
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Runnable r = new Runnable() {

            @Override
            public void run() {
                try {
                    for (int i = 0; i < 10000; ++i) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(1000);
                            String val = zc.getData("/test", false);
                            logger.debug("val: " + val);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    logger.debug(e.toString());
                }
            }
        };
        Thread thread = new Thread(r);
        thread.start();
        try {
            TimeUnit.MILLISECONDS.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    */
}
