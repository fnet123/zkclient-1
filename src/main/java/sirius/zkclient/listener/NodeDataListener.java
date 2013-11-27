package sirius.zkclient.listener;

/**
 * 
 * @author liyong19861014@gmail.com
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
