package com.qunar.zkclient.exception;

/**
 * 
 * @author michael
 * @email liyong19861014@gmail.com
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
