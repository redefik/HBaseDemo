package it.viglietta.federico.hbasejavademo.hbasebackend.exception;

public class DataOperationException extends Exception {

    public DataOperationException(String s) {
        super(s);
    }

    public DataOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
