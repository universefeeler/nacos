package com.alibaba.nacos.cluster.server.jraft.business;

/**
 * @Author: wanglei1
 * @Date: 2019/08/11 20:34
 * @Description:
 */
public class RequestRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RequestRuntimeException() {
    }

    public RequestRuntimeException(String message) {
        super(message);
    }

    public RequestRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public RequestRuntimeException(Throwable cause) {
        super(cause);
    }
}

