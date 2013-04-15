package com.dajie.core.zk.listener;

/**
 * 
 * @author liyong@dajie-inc.com
 *
 */
public abstract class NodeDataListener {

    private String nodePath = "";

    public NodeDataListener(String nodePath) {
        if (nodePath != null) {
            this.nodePath = nodePath;
        }
    }
    
    public String getNodePath() {
        return nodePath;
    }

    public abstract boolean update(String value);
    
    public abstract boolean delete();
}
