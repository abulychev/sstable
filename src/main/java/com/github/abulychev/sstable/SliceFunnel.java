package com.github.abulychev.sstable;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.nio.ByteBuffer;

/**
 * Created by abulychev on 24.06.15.
 */
public enum SliceFunnel implements Funnel<Slice> {
    INSTANCE;

    public void funnel(Slice from, PrimitiveSink into) {
        ByteBuffer buffer = from.toByteBuffer();
        while (buffer.hasRemaining()) {
            into.putByte(buffer.get());
        }
    }

    public static SliceFunnel getInstance() {
        return INSTANCE;
    }
}
