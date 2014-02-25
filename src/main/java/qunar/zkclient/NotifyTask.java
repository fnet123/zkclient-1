package qunar.zkclient;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import qunar.zkclient.util.ZkLogger;

/**
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class NotifyTask implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ZkLogger.class);

    private ZkClient zkClient;

    private BlockingQueue<Message> messages;

    public NotifyTask() {
        this.zkClient = null;
        this.messages = new LinkedBlockingQueue<Message>();
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    /**
     * 
     * @param nodePath
     */
    public void addMessage(Message msg) {
        try {
            messages.put(msg);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void process() {
        while (true) {
            Message msg = null;
            try {
                msg = messages.take();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                continue;
            }
            if (zkClient == null) {
                logger.error("zkClient == null");
                continue;
            }
            if (msg.getUpdatedCount() >= Message.MAX_UPDATED_COUNT) {
                logger.warn("message cannot be updated, nodePath:" + msg.getNodePath());
                continue;
            }
            boolean result = true;
            switch (msg.getType()) {
                case Message.NODE_CHILDREN_CHANGED:
                    result = zkClient.updateChildren(msg.getNodePath());
                    break;
                case Message.NODE_CREATED:
                    result = zkClient.updateNode(msg.getNodePath());
                    break;
                case Message.NODE_DELETED:
                    result = zkClient.deleteNode(msg.getNodePath());
                    break;
                case Message.NODE_DATA_CHANGED:
                    result = zkClient.updateNode(msg.getNodePath());
                    break;
                case Message.NODE_REFRESH:
                    zkClient.updateAllNodesAndChildren();
                    break;
                default:
                    break;
            }
            if (!result) { // if update failed, put msg into messages
                msg.incUpdatedCount();
                try {
                    messages.put(msg);
                } catch (InterruptedException e) {
                    //                    e.printStackTrace();
                }
            }
        }
    }

    public void run() {
        try {
            process();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

}
