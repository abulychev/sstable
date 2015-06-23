package com.github.abulychev.sstable;

import java.nio.ByteBuffer;

/**
 * Created by abulychev on 23.06.15.
 */
public class ByteBufferUtils {
    public static ByteBuffer slice(ByteBuffer data, int offset, int len) {
        return (ByteBuffer) data.duplicate().limit(offset+len).position(offset);
    }
}
