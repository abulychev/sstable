package com.github.abulychev.sstable;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by abulychev on 24.06.15.
 */
public class SliceTest {
    @Test
    public void test() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (int i = 0; i < 256; i++) {
            os.write(i);
        }

        byte bytes[] = os.toByteArray();

        Slice slice = Slice.wrap(bytes);
        assertTrue(sliceContains(slice, 0, 256));

        Slice slice2 = slice.subslice(3, 89);
        assertTrue(slice2.get(10) == 13);
        assertTrue(slice2.get(0) == 3);
        assertTrue(sliceContains(slice2, 3, 3 + 89));

        Slice slice3 = slice2.subslice(5, 10);
        assertTrue(sliceContains(slice3, 3 + 5, 3 + 5 + 10));

        Slice slice4 = slice2.subslice(1, 5);
        assertArrayEquals(slice4.toByteArray(), new byte[] { 4, 5, 6, 7, 8 });
    }

    @Test
    public void test2() {
        byte bytes1[] = {1, 2, 3, 4, 5};
        byte bytes2[] = {1, 2, 3, 10, 5};
        byte bytes3[] = {1, 2};

        Slice slice1 = Slice.wrap(bytes1);
        Slice slice2 = Slice.wrap(bytes2);
        Slice slice3 = Slice.wrap(bytes3);

        assertTrue(slice1.largestCommonPrefix(slice2) == 3);
        assertTrue(slice1.largestCommonPrefix(slice3) == 2);
        assertTrue(slice3.largestCommonPrefix(slice2) == 2);

        assertTrue(slice1.compareTo(slice2) < 0);
        assertTrue(slice2.compareTo(slice1) > 0);

        assertTrue(slice2.compareTo(slice2) == 0);
        assertTrue(slice3.compareTo(slice1) < 0);
        assertTrue(slice1.compareTo(slice3) > 0);
    }

    private static boolean sliceContains(Slice slice, int begin, int end) throws IOException {
        if (slice.size() != end - begin) {
            return false;
        }

        DataInputStream dis = new DataInputStream(slice.getContent());
        for (int i = begin; i < end; i++) {
            if (dis.available() == 0 || dis.readByte() != (byte) i) {
                return false;
            }
        }
        return dis.available() == 0;
    }
}
