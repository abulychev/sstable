package com.github.abulychev.sstable;

import com.google.common.hash.BloomFilter;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by abulychev on 22.06.15.
 */
public class SSTableWriter implements Closeable {
    private final DataOutputStream dos;

    /* Data */
    private int blockStart = 0;
    private byte[] blockKey = null;
    private int blockSize = 0;

    private byte[] pKey = null;

    /* Index */
    private final List<IndexEntry> indexEntries = new LinkedList<>();
    private int indexOffset = 0;

    /* Bloom filter */
    private int bloomFilterOffset = 0;

    public SSTableWriter(OutputStream os) {
        dos = new DataOutputStream(os);
    }

    public void closeBlock() {
        if (blockSize > 0) {
            IndexEntry e = new IndexEntry(blockStart, dos.size() - blockStart, blockKey);
            indexEntries.add(e);
        }

        blockStart = dos.size();
        blockKey = null;
        blockSize = 0;
        pKey = null;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getBlockCapacity() {
        return dos.size() - blockStart;
    }

    public int size() {
        return dos.size();
    }

    public void writeEntry(byte[] key, byte[] value) throws IOException {
        int lcp = pKey == null ? 0 : ByteUtils.largestCommonPrefix(pKey, key);

        VariableLengthQuantity.writeInt(lcp, dos);
        VariableLengthQuantity.writeInt(key.length - lcp, dos);
        VariableLengthQuantity.writeInt(value.length, dos);

        dos.write(key, lcp, key.length - lcp);
        dos.write(value);

        if (blockKey == null) blockKey = key;
        blockSize++;

        pKey = key;

    }

    public void writeIndex() throws IOException {
        indexOffset = dos.size();
        for (IndexEntry e: indexEntries) {
            writeIndexEntry(e.getOffset(), e.getSize(), e.getKey());
        }
    }

    public void writeIndexEntry(int offset, int size, byte[] key) throws IOException {
        dos.writeInt(offset);
        dos.writeInt(size);
        dos.writeInt(key.length);
        dos.write(key);
    }

    public void writeBloomFilter(BloomFilter<byte[]> filter) throws IOException {
        bloomFilterOffset = dos.size();
        filter.writeTo(dos);
    }

    public void writeFooter() throws IOException {
        int footerOffset = dos.size();

        dos.writeInt(indexOffset);
        dos.writeInt(bloomFilterOffset != 0 ? bloomFilterOffset : footerOffset);
        dos.writeInt(Constants.VERSION);
    }

    public void close() throws IOException {
        dos.close();
    }
}
