package com.github.abulychev.sstable;

/**
 * Created by abulychev on 22.06.15.
 */
public class Entry {
    private final Slice key;
    private final Slice value;

    public Entry(Slice key, Slice value) {
        this.key = key;
        this.value = value;
    }

    public Slice getKey() {
        return key;
    }

    public Slice getValue() {
        return value;
    }
}
