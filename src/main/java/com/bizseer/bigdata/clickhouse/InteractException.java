package com.bizseer.bigdata.clickhouse;
/**
 * @author xiebo
 * @date 2021/11/23 11:39 上午
 */
public class InteractException extends Exception{

    private String errMsg;

    private ConnectInfo connectInfo;

    public InteractException(String errMsg) {
        super(errMsg);
        this.errMsg = errMsg;
    }

    public InteractException(String message, String errMsg) {
        super(message);
        this.errMsg = errMsg;
    }

    public InteractException(Throwable cause) {
        super(cause);
    }

    public InteractException(String message, Throwable cause, String errMsg) {
        super(message, cause);
        this.errMsg = errMsg;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public ConnectInfo getConnectInfo() {
        return connectInfo;
    }

    public InteractException setConnectInfo(ConnectInfo connectInfo) {
        this.connectInfo = connectInfo;
        return this;
    }
}