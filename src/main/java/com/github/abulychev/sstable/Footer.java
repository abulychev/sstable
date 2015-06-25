package com.github.abulychev.sstable;

import com.google.common.primitives.Ints;

/**
 * Created by abulychev on 23.06.15.
 */
public class Footer {
    public static final int SIZE = 3 * Ints.BYTES;

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
