package com.github.abulychev.sstable;

import java.nio.ByteBuffer;

/**
 * Created by abulychev on 25.06.15.
 */
public class RawEntry {
    private final int shared;
    private final int nonshared;
    private final Slice nonsharedKey;
    private final Slice value;

    public RawEntry(int shared, int nonshared, Slice nonsharedKey, Slice value) {
        assert nonshared == nonsharedKey.size();

        this.shared = shared;
        this.nonshared = nonshared;
        this.nonsharedKey = nonsharedKey;
        this.value = value;
    }

    public int getShared() {
        return shared;
    }

    public Slice getKey(Slice previous) {
        if (shared > 0 && previous == null) {
            throw new IllegalArgumentException();
        }

        if (shared == 0) {
            return nonsharedKey;
        }

        byte key[] = new byte[shared + nonshared];

        ByteBuffer b1 = previous.toByteBuffer();
        b1.get(key, 0, shared);

        ByteBuffer b2 = nonsharedKey.toByteBuffer();
        b2.get(key, shared, nonshared);

        return Slice.wrap(key);
    }

    public Slice getNonSharedKey() {
        return nonsharedKey;
    }

    public Slice getValue() {
        return value;
    }
}
