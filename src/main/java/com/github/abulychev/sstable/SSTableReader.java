package com.github.abulychev.sstable;

import com.google.common.hash.BloomFilter;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by abulychev on 23.06.15.
 */
public class SSTableReader {
    private static final SSTableReader reader = new SSTableReader();

    public static SSTableReader getReader() {
        return reader;
    }

    private SSTableReader() {

    }

    public SSTable from(Slice data) throws IOException {
        int size = data.size();

        int footerOffset = size - Constants.FOOTER_SIZE;

        Slice footerData = data.subslice(footerOffset, Constants.FOOTER_SIZE);
        Footer footer = readFooter(footerData);

        if (footer.getVersion() > Constants.VERSION) {
            throw new IOException(String.format("Unsupported %d version of table", footer.getVersion()));
        }

        BloomFilter<Slice> bloomFilter;
        int bloomFilterSize = footerOffset - footer.getBloomFilterOffset();
        if (bloomFilterSize > 0) {
            Slice bloomFilterData = data.subslice(footer.getBloomFilterOffset(), bloomFilterSize);
            bloomFilter = readBloomFilter(bloomFilterData);
        } else {
            bloomFilter = null;
        }

        int indexSize = footer.getBloomFilterOffset() - footer.getIndexOffset();
        Slice indexData = data.subslice(footer.getIndexOffset(), indexSize);
        Index index = readIndex(indexData);

        return new SSTable(data.subslice(0, footer.getIndexOffset()), index, bloomFilter);
    }

    public SSTable from(ByteBuffer data) throws IOException {
        return from(Slice.wrap(data));
    }

    public SSTable from(File file) throws IOException {
        return from(file, false);
    }

    public SSTable from(File file, boolean force) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {

            int size = (int) channel.size();
            MappedByteBuffer data = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);

            if (force) {
                data.load();
            }

            return from(data);
        }
    }

    private static Footer readFooter(Slice data) throws IOException {
        DataInputStream is = new DataInputStream(data.getContent());

        int indexPosition = is.readInt();
        int bloomFilterPosition = is.readInt();
        int version = is.readInt();

        return new Footer(version, indexPosition, bloomFilterPosition);
    }

    private static BloomFilter<Slice> readBloomFilter(Slice data) throws IOException {
        return BloomFilter.readFrom(data.getContent(), SliceFunnel.getInstance());
    }

    private static Index readIndex(Slice data) throws IOException {
        return new Index(data);
    }
}
