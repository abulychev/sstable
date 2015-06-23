package com.github.abulychev.sstable;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

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

    public SSTable from(ByteBuffer data) throws IOException {
        int size = data.capacity();

        int footerOffset = size - Constants.FOOTER_SIZE;

        ByteBuffer footerData = ByteBufferUtils.slice(data, footerOffset, Constants.FOOTER_SIZE);
        Footer footer = readFooter(footerData);

        if (footer.getVersion() > Constants.VERSION) {
            throw new IOException(String.format("Unsupported %d version of table", footer.getVersion()));
        }

        BloomFilter<byte[]> bloomFilter;
        int bloomFilterSize = footerOffset - footer.getBloomFilterOffset();
        if (bloomFilterSize > 0) {
            ByteBuffer bloomFilterData = ByteBufferUtils.slice(data, footer.getBloomFilterOffset(), bloomFilterSize);
            bloomFilter = readBloomFilter(bloomFilterData);
        } else {
            bloomFilter = null;
        }

        int indexSize = footer.getBloomFilterOffset() - footer.getIndexOffset();
        ByteBuffer indexData = ByteBufferUtils.slice(data, footer.getIndexOffset(), indexSize);
        List<IndexEntry> blocks = readIndex(indexData);

        return new SSTable(data, blocks, bloomFilter);
    }

    public SSTable from(File file) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {

            int size = (int) channel.size();
            MappedByteBuffer data = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);

            return from(data);
        }
    }

    private static Footer readFooter(ByteBuffer data) throws IOException {
        DataInputStream is = new DataInputStream(new ByteBufferInputStream(data));

        int indexPosition = is.readInt();
        int bloomFilterPosition = is.readInt();
        int version = is.readInt();

        return new Footer(version, indexPosition, bloomFilterPosition);
    }

    private static BloomFilter<byte[]> readBloomFilter(ByteBuffer data) throws IOException {
        return BloomFilter.readFrom(new ByteBufferInputStream(data), Funnels.byteArrayFunnel());
    }

    private static List<IndexEntry> readIndex(ByteBuffer data) throws IOException {
        List<IndexEntry> blocks = new ArrayList<>();

        DataInputStream dis = new DataInputStream(new ByteBufferInputStream(data));
        while (dis.available() > 0) {
            int offset = dis.readInt();
            int size = dis.readInt();
            int l = dis.readInt();
            byte[] key = new byte[l];
            dis.read(key);
            blocks.add(new IndexEntry(offset, size, key));
        }

        return blocks;
    }
}
