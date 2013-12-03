package sirius.zkclient;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import sirius.zkclient.util.ZkLogger;

/**
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class ZkClientFactory {

    private static Logger logger = Logger.getLogger(ZkLogger.class);

    private static final Map<String, ZkClient> map = new HashMap<String, ZkClient>();

    public static ZkClient get(String zkAddress) {
        ZkClient client = null;
        synchronized (ZkClientFactory.class) {
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
}
