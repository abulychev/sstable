package com.github.abulychev.sstable;

import com.google.common.hash.BloomFilter;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by abulychev on 22.06.15.
 */
public class SSTable {
    private static final Comparator<byte[]> comparator = ByteUtils.getDefaultKeyComparator();

    private final ByteBuffer data;
    private final List<IndexEntry> blocks;
    private final BloomFilter<byte[]> bloomFilter;

    public SSTable(ByteBuffer data, List<IndexEntry> blocks, BloomFilter<byte[]> bloomFilter) throws IOException {
        this.data = data;
        this.blocks = blocks;
        this.bloomFilter = bloomFilter;
    }

    public byte[] get(byte[] key) throws IOException {
        if (bloomFilter != null && !bloomFilter.mightContain(key)) {
            return null;
        }

        IndexEntry h = findBlock(key);

        ByteBuffer block = ByteBufferUtils.slice(data, h.getOffset(), h.getSize());
        Iterator<Entry> it = new BlockIterator(block);
        while (it.hasNext()) {
            Entry e = it.next();
            int c = comparator.compare(key, e.getKey());
            if (c == 0) {
                return e.getValue();
            } else if (c < 0) {
                return null;
            }
        }

        return null;
    }

    private IndexEntry findBlock(byte[] key) {
        int l = 0, r = blocks.size() - 1;
        while (l < r) {
            int m = (l + r + 1) / 2;
            IndexEntry h = blocks.get(m);
            int c = comparator.compare(key, h.getKey());

            if (c >= 0) {
                l = m;
                if (c == 0) r = m;
            } else {
                r = m - 1;
            }
        }
        return blocks.get(l);
    }
}
