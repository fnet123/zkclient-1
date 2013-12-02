package sirius.zkclient;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import sirius.zkclient.util.ZkLogger;

/**
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class NotifyTask implements Runnable {

    private static Logger logger = Logger.getLogger(ZkLogger.class);

    private ZkClient zkClient;

    private BlockingDeque<Message> messages;

    public NotifyTask() {
        this.zkClient = null;
        this.messages = new LinkedBlockingDeque<Message>();
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    /**
     * 异步实现此方法
     * 
     * @param nodePath
     */
    public void addMessage(Message msg) {
        try {
            messages.put(msg);
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    public void run() {
        while (true) {
            Message msg = null;
            try {
                msg = messages.take();
            } catch (InterruptedException e) {
                logger.error(e);
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
                messages.push(msg);
            }
        }
    }

}
