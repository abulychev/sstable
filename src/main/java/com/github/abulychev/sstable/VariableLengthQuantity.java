package com.github.abulychev.sstable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by abulychev on 22.06.15.
 */
public final class VariableLengthQuantity {
    private static final int highBitMask = 0x80;

    public static void writeInt(int value, OutputStream os) throws IOException {
        while ((value & ~0x7f) != 0) {
            int v = (value | highBitMask) & 0xff;
            value = value >>> 7;
            os.write(v);
        }
        os.write(value);
    }

    public static int readInt(InputStream is) throws IOException {
        int value = 0, shift = 0, v;
        do {
            v = is.read();
            value = value | ((v & 0x7f) << shift);
            shift += 7;
        } while (v >= highBitMask);
        return value;
    }
}
