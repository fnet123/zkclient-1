package com.dajie.core.zk.exception;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class ZkException extends Exception {

    private static final long serialVersionUID = 1L;

    public ZkException(String msg) {
        super(msg);
    }

    public ZkException(String msg, Throwable t) {
        super(msg, t);
    }

    public ZkException(Throwable cause) {
        super(cause);
    }
}
