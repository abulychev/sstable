package com.github.abulychev.sstable;

import com.google.common.hash.BloomFilter;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by abulychev on 22.06.15.
 */
public class TableWriter implements Closeable {
    private final DataOutputStream dos;

    /* Data */
    private int blockStart = 0;
    private int blockSize = 0;

    private Slice pKey = null;

    /* Index */
    private final List<BlockHandle> indexEntries = new LinkedList<>();
    private int indexOffset = 0;

    /* Bloom filter */
    private int bloomFilterOffset = 0;

    public TableWriter(OutputStream os) {
        dos = new DataOutputStream(os);
    }

    public void closeBlock() {
        if (blockSize > 0) {
            BlockHandle e = new BlockHandle(blockStart, dos.size() - blockStart);
            indexEntries.add(e);
        }

        blockStart = dos.size();
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

    public void writeEntry(Slice key, Slice value) throws IOException {
        int lcp = pKey == null ? 0 : pKey.largestCommonPrefix(key);

        Slice nonshared = key.subslice(lcp);

        VariableLengthQuantity.writeInt(lcp, dos);
        VariableLengthQuantity.writeInt(key.size() - lcp, dos);
        VariableLengthQuantity.writeInt(value.size(), dos);

        IOUtils.copy(nonshared.getContent(), dos);
        IOUtils.copy(value.getContent(), dos);

        blockSize++;

        pKey = key;
    }

    public void writeIndex() throws IOException {
        indexOffset = dos.size();
        for (BlockHandle h: indexEntries) {
            writeBlockHandle(h);
        }
    }

    public void writeBlockHandle(BlockHandle handle) throws IOException {
        dos.writeInt(handle.getOffset());
        dos.writeInt(handle.getSize());
    }

    public void writeBloomFilter(BloomFilter<Slice> filter) throws IOException {
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
