package com.bizseer.bigdata.metadataInfluxdb;

/**
 * @author zhangyingjie
 */
public class DalServerException extends RuntimeException {
    private String message;

    public DalServerException(String message) {
        this.message = message;
    }
}
