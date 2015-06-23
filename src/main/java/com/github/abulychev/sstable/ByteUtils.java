package com.github.abulychev.sstable;

import java.util.Comparator;

/**
 * Created by abulychev on 22.06.15.
 */
public class ByteUtils {
    public static int largestCommonPrefix(byte[] a, byte[] b) {
        if (a.length > b.length) {
            return largestCommonPrefix(b, a);
        }

        int i = 0;
        while (i < a.length && a[i] == b[i]) i++;

        return i;
    }

    public static Comparator<byte[]> getDefaultKeyComparator() {
        return defaultKeyComparator;
    }

    private static final Comparator<byte[]> defaultKeyComparator = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] a, byte[] b) {
            int i = largestCommonPrefix(a, b);

            if (i == a.length && i == b.length) {
                return 0;
            } else if (i == a.length) {
                return -1;
            } else if (i == b.length) {
                return 1;
            } else {
                return Byte.compare(a[i], b[i]);
            }
        }
    };
}
