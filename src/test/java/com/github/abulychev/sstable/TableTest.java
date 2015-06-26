package com.github.abulychev.sstable;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by abulychev on 23.06.15.
 */
@RunWith(Parameterized.class)
public class TableTest {
    private static final int N = 1000000;

    private static final Random r = new Random(1);

    private static File file;
    private static Map<byte[], byte[]> map;
    private final boolean bloom;

    private Table table;

    public TableTest(boolean bloom) {
        this.bloom = bloom;
    }

    @Parameterized.Parameters
    public static List<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                { false },
                { true }
        });
    }

    @BeforeClass
    public static void beforeClass() {
        map = randomMap(N);
        file = new File("data.sst");
    }

    @Before
    public void before() throws Exception {
        saveMapTo(map, file, bloom);
        table = TableReader.getReader().from(file);
    }

    @After
    public void after() throws Exception {
        file.delete();
    }

    @Test
    public void test() throws Exception {
        for (Map.Entry<byte[], byte[]> e: map.entrySet()) {
            byte[] key = e.getKey(), value = e.getValue();
            byte[] v = table.get(key);

            assertTrue(v != null);
            assertArrayEquals(value, v);
        }
    }

    @Test
    public void test2() throws Exception {
        for (int i = 0; i < N; i++) {
            byte[] key = randomKey();
            assertTrue(table.get(key) == null);
        }
    }

    private static Map<byte[], byte[]> randomMap(int size) {
        Map<byte[], byte[]> m = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            byte[] key = randomKey();
            byte[] value = randomValue();
            m.put(key, value);
        }

        return m;
    }

    private static void saveMapTo(Map<byte[], byte[]> m, File file, boolean bloom) throws IOException {
        TableBuilder builder = new TableBuilder();
        builder.setUseBloomFilter(bloom);
        builder.setMaxBlockSize(32);

        for (Map.Entry<byte[], byte[]> e: m.entrySet()) {
            byte[] key = e.getKey(), value = e.getValue();
            builder.put(key, value);
        }

        builder.writeTo(file);
    }

    private static byte[] randomKey() {
        return randomString(15, 20).getBytes();
    }

    private static byte[] randomValue() {
        return randomString(10, 100).getBytes();
    }

    private static String randomString(int min, int max) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < min + r.nextInt(max - min + 1); i++) {
            sb.append((char) ('a' + r.nextInt(26)));
        }
        return sb.toString();
    }
}
