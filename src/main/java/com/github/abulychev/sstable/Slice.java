package com.github.abulychev.sstable;

import java.nio.ByteBuffer;

/**
 * Created by abulychev on 24.06.15.
 */
public class Slice implements Comparable<Slice> {
    private final ByteBuffer data;
    private final int offset;
    private final int size;

    private Slice(ByteBuffer data, int offset, int size) {
        assert offset >= 0;
        assert offset + size <= data.limit();

        this.data = data;
        this.offset = offset;
        this.size = size;
    }

    public static Slice wrap(ByteBuffer buffer) {
        return new Slice(buffer, buffer.position(), buffer.limit() - buffer.position());
    }

    public static Slice wrap(ByteBuffer buffer, int offset, int size) {
        return new Slice(buffer, offset, size);
    }

    public static Slice wrap(byte[] bytes) {
        return wrap(ByteBuffer.wrap(bytes));
    }

    public int size() {
        return size;
    }

    public Slice subslice(int offset, int size) {
        return new Slice(data, this.offset + offset, size);
    }

    public Slice subslice(int n) {
        if (n == 0) {
            return this;
        }
        return subslice(n, size - n);
    }

    public byte get(int index) {
        assert index < size;
        return data.get(offset + index);
    }

    public int getInt(int index) {
        assert index + 3 < size;
        return data.getInt(offset + index);
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer duplicate = data.duplicate();
        duplicate.limit(offset + size);
        duplicate.position(offset);
        return duplicate;
    }

    public byte[] toByteArray() {
        ByteBuffer duplicate = toByteBuffer();
        byte bytes[] = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    public SliceInputStream getContent() {
        return new SliceInputStream(this);
    }

    public int largestCommonPrefix(Slice that) {
        if (this == that) {
            return this.size;
        }

        int l = Math.min(this.size, that.size);
        if (this.data == that.data && this.offset == that.offset) {
            return l;
        }

        ByteBuffer b1 = this.toByteBuffer();
        ByteBuffer b2 = that.toByteBuffer();

        int i = 0;
        while (i < l) {
            int a = 0xff & b1.get();
            int b = 0xff & b2.get();
            if (a != b) break;
            i++;
        }

        return i;
    }

    public int compareTo(Slice that) {
        if (this == that) {
            return 0;
        }
        if (this.data == that.data && this.offset == that.offset && this.size == that.size) {
            return 0;
        }

        ByteBuffer b1 = this.toByteBuffer();
        ByteBuffer b2 = that.toByteBuffer();

        return compare(b1, b2);
    }

    private static int compare(ByteBuffer b1, ByteBuffer b2) {
        int pos1 = b1.position(), r1 = b1.remaining();
        int pos2 = b2.position(), r2 = b2.remaining();

        int n = pos1 + Math.min(r1, r2) ;

        for (int i = pos1, j = pos2; i < n; i++, j++) {
            int cmp = compare(b1.get(i), b2.get(j));

            if (cmp != 0) {
                int r = (i - pos1 + 1);
                return (cmp < 0) ? -r : r;
            }
        }

        int r = n - pos1 + 1;
        return (r1 == r2) ? 0 :
                ((r1 < r2) ? -r : r);
    }

    private static int compare(byte x, byte y) {
        return Byte.compare(x, y);
    }
}
