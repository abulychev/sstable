package com.github.abulychev.sstable;

/**
 * Created by abulychev on 23.06.15.
 */
public class Footer {
    private final int version;
    private final int indexOffset;
    private final int bloomFilterOffset;

    public Footer(int version, int indexOffset, int bloomFilterOffset) {
        this.version = version;
        this.indexOffset = indexOffset;
        this.bloomFilterOffset = bloomFilterOffset;
    }

    public int getVersion() {
        return version;
    }

    public int getIndexOffset() {
        return indexOffset;
    }

    public int getBloomFilterOffset() {
        return bloomFilterOffset;
    }
}
