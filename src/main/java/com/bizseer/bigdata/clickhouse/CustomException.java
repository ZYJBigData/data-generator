package com.bizseer.bigdata.clickhouse;

public class CustomException extends RuntimeException {
    private String err;

    public CustomException(String message, String err) {
        super(message);
        this.err = err;
    }

    public CustomException(String err) {
        super(err);
        this.err = err;
    }

    public CustomException(Throwable cause) {
        super(cause);
        this.err = cause.getMessage();
    }

    public CustomException(String message, Throwable cause, String err) {
        super(message, cause);
        this.err = err;
    }

    public String getErr() {
        return this.err;
    }
}
