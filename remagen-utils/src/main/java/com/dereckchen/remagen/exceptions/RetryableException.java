package com.dereckchen.remagen.exceptions;

public class RetryableException extends RuntimeException {
    public RetryableException(Throwable cause) {
        super(cause);
    }

    public RetryableException(String format, String... args) {
        super(String.format(format, args));
    }
}
