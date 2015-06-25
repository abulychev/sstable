package com.github.abulychev.sstable;

import com.google.common.primitives.Ints;

/**
 * Created by abulychev on 25.06.15.
 */
public class Index {
    private final Slice data;
    private final int size;

    public Index(Slice data) {
        this.data = data;
        this.size = data.size() / BlockHandle.SIZE;
    }

    public int size() {
        return size;
    }

    public BlockHandle get(int index) {
        assert index < size;

        int offset = index * BlockHandle.SIZE;
        return new BlockHandle(
                data.getInt(offset),
                data.getInt(offset + Ints.BYTES)
        );
    }
}
