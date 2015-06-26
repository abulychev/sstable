package com.github.abulychev.sstable;

import com.google.common.hash.BloomFilter;

import java.io.*;
import java.util.*;

/**
 * Created by abulychev on 22.06.15.
 */
public class Table implements Iterable<Entry> {
    private final Slice data;
    private final Index index;
    private final BloomFilter<Slice> bloomFilter;

    public Table(Slice data, Index index, BloomFilter<Slice> bloomFilter) throws IOException {
        this.data = data;
        this.index = index;
        this.bloomFilter = bloomFilter;
    }

    public byte[] get(byte[] key) throws IOException {
        return get(Slice.wrap(key));
    }

    public byte[] get(Slice key) throws IOException {
        if (bloomFilter != null && !bloomFilter.mightContain(key)) {
            return null;
        }

        int blockIndex = findBlock(key);
        Iterator<RawEntry> it = createRawEntryIterator(blockIndex);

        int prefix = 0;
        while (it.hasNext()) {
            RawEntry e = it.next();

            if (e.getShared() < prefix) {
                return null;
            } else if (e.getShared() == prefix) {
                int c = key.compareTo(e.getNonSharedKey());
                if (c == 0) {
                    return e.getValue().toByteArray();
                } else if (c < 0) {
                    return null;
                } else {
                    prefix += c - 1;
                    key = key.subslice(c - 1);
                }
            }
        }

        return null;
    }

    private int findBlock(Slice key) {
        int l = 0, r = index.size() - 1;

        Slice lKey = firstKeyOfBlock(l);
        Slice rKey = firstKeyOfBlock(r);

        int c1 = key.compareTo(lKey);
        if (c1 <= 0) {
            return 0;
        }

        int c2 = key.compareTo(rKey);
        if (c2 >= 0) {
            return r;
        }

        int p1 = c1 - 1, p2 = -c2 - 1;
        while (l < r) {
            int p = Math.min(p1, p2);
            Slice sKey = key.subslice(p);

            int m = (l + r + 1) / 2;
            Slice mKey = firstKeyOfBlock(m).subslice(p);

            int c = sKey.compareTo(mKey);
            if (c == 0) {
                return m;
            } else if (c > 0) {
                l = m;
                p1 = p + (c - 1);
            } else {
                r = m - 1;
                p2 = p + (-c - 1);
            }
        }
        return l;
    }

    private Slice firstKeyOfBlock(int blockIndex) {
        Iterator<RawEntry> it = createRawEntryIterator(blockIndex);
        return it.next().getKey(null);
    }

    private Iterator<RawEntry> createRawEntryIterator(int blockIndex) {
        BlockHandle handle = index.get(blockIndex);
        Slice block = data.subslice(handle.getOffset(), handle.getSize());
        return new RawEntryIterator(block);
    }

    private Iterator<Entry> iterator(int blockIndex) {
        Iterator<RawEntry> it = createRawEntryIterator(blockIndex);
        return new EntryIterator(it);
    }

    public Iterator<Entry> iterator() {
        return new EntryIterator(data);
    }
}
