package com.dereckchen.remagen.exceptions;

public class RetryableException extends RuntimeException {
    /**
     * Constructs a new runtime exception with the specified cause.
     *
     * @param cause the cause
     */
    public RetryableException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new runtime exception with the specified format string and arguments.
     *
     * @param format the format string
     * @param args   the arguments
     */
    public RetryableException(String format, String... args) {
        super(String.format(format, args));
    }
}

