package com.github.abulychev.sstable;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Created by abulychev on 23.06.15.
 */
public class BlockIterator implements Iterator<Entry> {
    private final DataInputStream dis;
    byte[] pKey = null;

    public BlockIterator(ByteBuffer buffer) {
        dis = new DataInputStream(new ByteBufferInputStream(buffer));
    }

    public Entry next() {
        try {
            int lcp = VariableLengthQuantity.readInt(dis);
            int l = VariableLengthQuantity.readInt(dis);
            int valueLength = VariableLengthQuantity.readInt(dis);
            byte[] key = new byte[lcp + l], value = new byte[valueLength];

            if (pKey != null) {
                System.arraycopy(pKey, 0, key, 0, lcp);
            }
            // TODO: check the result
            dis.read(key, lcp, l);

            // TODO: do not read everything. We only need the last
            dis.read(value);

            pKey = key;

            return new Entry(key, value);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasNext() {
        try {
            return dis.available() > 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
