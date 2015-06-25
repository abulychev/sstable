package com.github.abulychev.sstable;

import com.google.common.primitives.Ints;

/**
 * Created by abulychev on 22.06.15.
 */
public class BlockHandle {
    public static final int SIZE = 2 * Ints.BYTES;

    private final int offset;
    private final int size;

    public BlockHandle(int offset, int size) {
        this.offset = offset;
        this.size = size;
    }

    public int getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }
}
