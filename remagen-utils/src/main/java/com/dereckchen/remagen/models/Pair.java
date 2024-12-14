package com.dereckchen.remagen.models;

/**
 * To adapt both in Java8 and Java11, we need to custom our Pair#Class
 *
 * @param <K> key type
 * @param <V> value type
 */
public class Pair<K, V> {
    private K key;
    private V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}
