package com.github.abulychev.sstable;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Created by abulychev on 22.06.15.
 */
public class SSTableBuilder {
    private final ArrayList<Entry> data = new ArrayList<>();
    private int maxBlockSize = 32;
    private int maxBlockCapacity = 64 * 1024;
    private boolean useBloomFilter = false;

    public SSTableBuilder setMaxBlockSize(int maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
        return this;
    }

    public void setMaxBlockCapacity(int maxBlockCapacity) {
        this.maxBlockCapacity = maxBlockCapacity;
    }

    public SSTableBuilder setUseBloomFilter(boolean useBloomFilter) {
        this.useBloomFilter = useBloomFilter;
        return this;
    }

    public SSTableBuilder put(byte[] key, byte[] value) {
        data.add(new Entry(key, value));
        return this;
    }

    public void writeTo(OutputStream os) throws IOException {
        try (SSTableWriter writer = new SSTableWriter(os)) {
            Collections.sort(data, comparator);

            BloomFilter<byte[]> bloomFilter = useBloomFilter ?
                    BloomFilter.create(Funnels.byteArrayFunnel(), data.size()) : null;


            for (Entry e: data) {
                byte[] key = e.getKey(), value = e.getValue();

                writer.writeEntry(key, value);

                if (isBlockFull(writer)) {
                    writer.closeBlock();
                }

                if (bloomFilter != null) {
                    bloomFilter.put(key);
                }
            }

            writer.closeBlock();

            writer.writeIndex();

            if (bloomFilter != null) {
                writer.writeBloomFilter(bloomFilter);
            }

            writer.writeFooter();
        }
    }

    public void writeTo(File file) throws IOException {
        try (OutputStream os = new FileOutputStream(file)) {
            writeTo(os);
        }
    }


    private boolean isBlockFull(SSTableWriter writer) {
        if (maxBlockSize != -1 && maxBlockSize <= writer.getBlockSize()) {
            return true;
        }
        if (maxBlockCapacity != -1 && maxBlockCapacity <= writer.getBlockCapacity()) {
            return true;
        }
        return false;
    }

    private static final Comparator<Entry> comparator = new Comparator<Entry>() {
        @Override
        public int compare(Entry o1, Entry o2) {
            return ByteUtils.getDefaultKeyComparator().compare(o1.getKey(), o2.getKey());
        }
    };
}
