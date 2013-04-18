package com.dajie.core.zk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.dajie.core.zk.exception.ZkException;
import com.dajie.core.zk.listener.NodeChildrenListener;
import com.dajie.core.zk.listener.NodeDataListener;
import com.dajie.core.zk.util.ZkLogger;

/**
 * 
 * @author liyong@dajie-inc.com
 *
 */
public class ZkClient {

    private static final Logger logger = Logger.getLogger(ZkLogger.class);

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
    private Watcher watcher;

    private String zkAddress;

    private ZooKeeper zk;

    private NotifyTask notifyTask;

    private Thread notifyThread;

    /** zk resource done */

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
                logger.error(e);
            }
        }

        notifyTask = new NotifyTask();
        notifyTask.setZkClient(this);
        notifyThread = new Thread(notifyTask);
        notifyThread.setDaemon(true);
        notifyThread.start();
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
            newValue = getData(nodePath);
        } catch (ZkException e) {
            logger.error(e);
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
            newChildren = getChildren(parentNodePath);
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
     * read
     * 
     * @param path
     * @return
     * @throws ZkException
     */
    public String getData(String path) throws ZkException {
        if (!exist(path)) {
            return null;
        }
        String value = null;
        synchronized (watcher) {
            Stat stat = new Stat();
            byte[] data = null;
            try {
                data = zk.getData(path, true, stat);
                logger.debug("stat: " + stat);
            } catch (Exception e) {
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
     * @return
     * @throws ZkException
     */
    public boolean exist(String path) throws ZkException {
        synchronized (watcher) {
            try {
                Stat stat = zk.exists(path, true);
                if (stat != null) {
                    return true;
                }
            } catch (Exception e) {
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
     * @return
     * @throws ZkException
     */
    public List<String> getChildren(String path) throws ZkException {
        if (!exist(path)) {
            return null;
        }
        List<String> children = null;
        synchronized (watcher) {
            try {
                children = zk.getChildren(path, true);
            } catch (Exception e) {
                logger.error("ZkClient.getChildren() path: " + path, e);
                throw new ZkException(e);
            }
        }
        return children;
    }

    // TODO: deleteNode method???  let me think about it for some while!!!
    // TODO: deleteAllChildren method???  let me think about it for some while!!!

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
            } catch (Exception e) {
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
            } catch (Exception e) {
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
        if (!exist(path)) {
            return false;
        }
        synchronized (watcher) {
            try {
                Stat stat = zk.setData(path, data.getBytes(), -1);
                logger.debug("ZkClient.setData() stat:" + stat);
                return true;
            } catch (Exception e) {
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

        public void process(WatchedEvent event) {

            System.out.println("event:" + event);

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
                                notifyTask.addMessage(new Message("", Message.NODE_REFRESH));
                                logger.debug("new zk started!");
                            }
                            return;
                        case Disconnected:
                            break;
                        case Expired:
                            while (true) {
                                closeZkConnection();
                                watcher = new ZkWatcher(false);
                                synchronized (watcher) {
                                    try {
                                        zk = new ZooKeeper(zkAddress, SESSION_TIMEOUT, watcher);
                                        watcher.wait();
                                        break;
                                    } catch (Exception e) {
                                        logger.error("ZkWatcher.process() Session Expired!", e);
                                    }
                                }
                            }
                        default:
                            break;
                    }
                    break;
                case NodeChildrenChanged:
                    if (!path.isEmpty()) {
                        notifyTask.addMessage(new Message(path, Message.NODE_CHILDREN_CHANGED));
                        try {
                            exist(path);
                        } catch (ZkException e) {
                            logger.error(e);
                        }
                    }
                    break;
                case NodeDeleted:
                    if (!path.isEmpty()) {
                        notifyTask.addMessage(new Message(path, Message.NODE_DELETED));
                        try {
                            exist(path);
                        } catch (ZkException e) {
                            logger.error(e);
                        }
                    }
                    break;
                case NodeCreated:
                    if (!path.isEmpty()) {
                        notifyTask.addMessage(new Message(path, Message.NODE_CREATED));
                        try {
                            exist(path);
                        } catch (ZkException e) {
                            logger.error(e);
                        }
                    }
                    break;
                case NodeDataChanged:
                    if (!path.isEmpty()) {
                        notifyTask.addMessage(new Message(path, Message.NODE_DATA_CHANGED));
                        try {
                            exist(path);
                        } catch (ZkException e) {
                            logger.error(e);
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        String zkAddress = "127.0.0.1:2181";
        ZkClient client = new ZkClient(zkAddress);
        client.addNodeChildrenListener(new NodeChildrenListener("/test3") {

            @Override
            public boolean update(List<String> childrenNameList) {
                System.out.println("nodePath:" + getNodePath() + ", childrenNameList: "
                        + childrenNameList);
                return true;
            }
        });
        client.addNodeDataListener(new NodeDataListener("/test4") {

            @Override
            public boolean update(String value) {
                System.out.println("nodePath: " + getNodePath() + ", value: " + value);
                return true;
            }

            @Override
            public boolean delete() {
                return true;
            }
        });
        try {
            System.out.println(client.getChildren("/test3"));
            System.out.println(client.getData("/test4"));
            TimeUnit.MINUTES.sleep(10);
        } catch (Exception e) {
            e.printStackTrace();
        }
        client.close();
    }
}
