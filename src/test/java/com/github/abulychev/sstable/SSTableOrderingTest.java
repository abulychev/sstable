package com.github.abulychev.sstable;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by abulychev on 25.06.15.
 */
public class SSTableOrderingTest {
    private static final File file = new File("data.sst");

    @Test
    public void test() throws IOException {
        SSTableBuilder builder = new SSTableBuilder();
        for (int i = 999; i >= 100; i--) {
            String key = String.valueOf(i);
            builder.put(key.getBytes(), key.getBytes());
        }
        builder.writeTo(file);

        SSTable table = SSTableReader.getReader().from(file);

        int j = 100;
        for (Entry e: table) {
            byte[] expect = String.valueOf(j++).getBytes();
            assertArrayEquals(e.getKey().toByteArray(), expect);
            assertArrayEquals(e.getValue().toByteArray(), expect);
        }
        assertTrue(j == 1000);
    }

}
