package com.github.abulychev.sstable;

/**
 * Created by abulychev on 22.06.15.
 */
public class Entry {
    private final byte[] key;
    private final byte[] value;

    public Entry(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
