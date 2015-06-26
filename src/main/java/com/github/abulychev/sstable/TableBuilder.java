package com.github.abulychev.sstable;

import com.google.common.hash.BloomFilter;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Created by abulychev on 22.06.15.
 */
public class TableBuilder {
    private final ArrayList<Entry> data = new ArrayList<>();
    private int maxBlockSize = 16;
    private int maxBlockCapacity = 4 * 1024;
    private boolean useBloomFilter = false;

    public TableBuilder setMaxBlockSize(int maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
        return this;
    }

    public void setMaxBlockCapacity(int maxBlockCapacity) {
        this.maxBlockCapacity = maxBlockCapacity;
    }

    public TableBuilder setUseBloomFilter(boolean useBloomFilter) {
        this.useBloomFilter = useBloomFilter;
        return this;
    }

    public TableBuilder put(byte[] key, byte[] value) {
        data.add(new Entry(Slice.wrap(key), Slice.wrap(value)));
        return this;
    }

    public void writeTo(OutputStream os) throws IOException {
        try (TableWriter writer = new TableWriter(os)) {
            Collections.sort(data, comparator);

            BloomFilter<Slice> bloomFilter = useBloomFilter ?
                    BloomFilter.create(SliceFunnel.getInstance(), data.size()) : null;

            for (Entry e: data) {
                Slice key = e.getKey(), value = e.getValue();

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


    private boolean isBlockFull(TableWriter writer) {
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
            return o1.getKey().compareTo(o2.getKey());
        }
    };
}
