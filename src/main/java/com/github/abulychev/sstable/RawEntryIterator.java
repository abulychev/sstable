package com.github.abulychev.sstable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by abulychev on 23.06.15.
 */
public class RawEntryIterator implements Iterator<RawEntry> {
    private final SliceInputStream is;

    public RawEntryIterator(Slice slice) {
        is = new SliceInputStream(slice);
    }

    public RawEntry next() {
        try {
            int shared = VariableLengthQuantity.readInt(is);
            int nonshared = VariableLengthQuantity.readInt(is);
            int valueLength = VariableLengthQuantity.readInt(is);

            Slice dKey = is.readSlice(nonshared);
            Slice value = is.readSlice(valueLength);

            return new RawEntry(shared, nonshared, dKey, value);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasNext() {
        try {
            return is.available() > 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
