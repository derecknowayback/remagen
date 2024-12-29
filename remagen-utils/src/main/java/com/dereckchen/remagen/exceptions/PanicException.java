package com.dereckchen.remagen.exceptions;

public class PanicException extends Exception {
    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public PanicException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified format string and arguments.
     *
     * @param format the format string
     * @param args   the arguments
     */
    public PanicException(String format, String... args) {
        super(String.format(format, args));
    }
}

