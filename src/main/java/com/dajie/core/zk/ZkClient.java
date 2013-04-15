package com.dajie.core.zk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.dajie.core.zk.exception.ZkException;
import com.dajie.core.zk.listener.NodeChildrenListener;
import com.dajie.core.zk.listener.NodeDataListener;
import com.dajie.core.zk.util.ZkLogger;

public class ZkClient {

    private static final Logger logger = Logger.getLogger(ZkLogger.class);

    private static final String UPDATE_ALL_NAMESPACE = "HOLY_SHIT_UPDATE_ALL_NAMESPACE";

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
        // interrupt notifyThread
        if (notifyThread != null) {
            try {
                logger.debug("notifyThread.isInterrupted(): " + notifyThread.isInterrupted());
                notifyThread.interrupt();
            } catch (Exception e) {
                logger.error("ZkClient.close()", e);
            }
        }
        // release zk resource
        if (zk != null) {
            try {
                logger.debug("close zk!");
                zk.close();
            } catch (Exception e) {
                logger.error("ZkClient.close()", e);
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
        return true;
    }

    /**
     * 
     * @param nodePath
     */
    public boolean deleteNode(String nodePath) {
        return true;
    }

    /**
     * 
     * @param parentNodePath
     */
    boolean updateChildren(String parentNodePath) {
        return true;
    }

    /**
     * 
     */
    boolean updateAllNodesAndChildren() {
        return true;
    }

    /**
     * read
     * 
     * @param path
     * @return
     * @throws ZkException
     */
    public String getData(String path) throws ZkException {
        String value = null;
        synchronized (watcher) {
            Stat stat = new Stat();
            byte[] data = null;
            try {
                data = zk.getData(path, true, stat);
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
            if (eventType.equals(EventType.None)) {
                switch (state) {
                    case SyncConnected:
                        synchronized (this) {
                            this.notifyAll();
                        }
                        if (!first) {
                            // after last session Expired 
                            //                            notifyTask.addMessage(UPDATE_ALL_NAMESPACE);
                        }
                        return;
                    case Disconnected:
                        break;
                    case Expired:
                        while (true) {
                            close();
                            watcher = new ZkWatcher(false);
                            synchronized (watcher) {
                                try {
                                    zk = new ZooKeeper(zkAddress, SESSION_TIMEOUT, watcher);
                                    watcher.wait();
                                } catch (Exception e) {
                                    logger.error("ZkWatcher.process() Session Expired!", e);
                                }
                            }
                        }
                    default:
                        break;
                }
            } else if (eventType.equals(EventType.NodeChildrenChanged)) {

            } else if (eventType.equals(EventType.NodeDeleted)) {

            } else if (eventType.equals(EventType.NodeCreated)) {

            } else if (eventType.equals(EventType.NodeDataChanged)) {
                if (!path.isEmpty()) {
                    // TODO: exists
                    synchronized (watcher) {
                        try {
                            zk.exists(path, true);
                        } catch (KeeperException e) {
                            logger.error(e);
                        } catch (InterruptedException e) {
                            logger.error(e);
                        }
                    }
                    //                    notifyTask.addMessage(path);
                }
            }

        }
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        String zkAddress = "127.0.0.1:2181";
        ZkClient client = new ZkClient(zkAddress);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client.close();
    }
}
