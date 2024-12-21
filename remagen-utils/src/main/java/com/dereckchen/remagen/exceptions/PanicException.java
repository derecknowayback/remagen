package com.dereckchen.remagen.exceptions;

public class PanicException extends Exception {
    public PanicException(String message, Throwable cause) {
        super(message, cause);
    }

    public PanicException(String format, String... args) {
        super(String.format(format, args));
    }
}
