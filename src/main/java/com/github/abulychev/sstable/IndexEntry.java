package com.github.abulychev.sstable;

/**
 * Created by abulychev on 22.06.15.
 */
public class IndexEntry {
    private final int offset;
    private final int size;
    private final byte[] key;

    public IndexEntry(int offset, int size, byte[] key) {
        this.offset = offset;
        this.size = size;
        this.key = key;
    }

    public int getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public byte[] getKey() {
        return key;
    }
}
