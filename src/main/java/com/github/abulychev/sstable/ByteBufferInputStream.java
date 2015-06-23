package com.github.abulychev.sstable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by abulychev on 23.06.15.
 */
public class ByteBufferInputStream extends InputStream {
    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public int read() throws IOException {
        if (!buffer.hasRemaining()) return -1;
        return buffer.get() & 0xff;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int k = Math.min(buffer.remaining(), len);
        if (k == 0) return -1;
        buffer.get(b, off, k);
        return k;
    }

    @Override
    public long skip(long n) {
        int k = Math.min((int) n, buffer.remaining());
        buffer.position(buffer.position() + k);
        return k;
    }

    public int available() throws IOException {
        return buffer.remaining();
    }
}
