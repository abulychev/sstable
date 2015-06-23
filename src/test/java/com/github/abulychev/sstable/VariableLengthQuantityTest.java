package com.github.abulychev.sstable;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Created by abulychev on 22.06.15.
 */
public class VariableLengthQuantityTest {
    private static final int T = 100;

    @Test
    public void test() throws Exception {
        Random r = new Random();
        for (int i = 0; i < T; i++) {
            int value = r.nextInt();

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            VariableLengthQuantity.writeInt(value, os);

            byte[] data = os.toByteArray();

            ByteArrayInputStream is = new ByteArrayInputStream(data);
            int v = VariableLengthQuantity.readInt(is);

            assertTrue(value == v);
        }
    }
}
