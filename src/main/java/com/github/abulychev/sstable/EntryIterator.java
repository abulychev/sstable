package com.github.abulychev.sstable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by abulychev on 23.06.15.
 */
public class EntryIterator implements Iterator<Entry> {
    private final Iterator<RawEntry> it;

    private Slice previousKey = null;

    public EntryIterator(Iterator<RawEntry> it) {
        this.it = it;
    }

    public EntryIterator(Slice slice) {
        this(new RawEntryIterator(slice));
    }

    public Entry next() {
        try {
            RawEntry entry = it.next();

            if (entry.getShared() != 0 && previousKey == null) {
                throw new IOException("Invalid block: non zero shared prefix");
            }

            Slice key = entry.getKey(previousKey);

            previousKey = key;

            return new Entry(key, entry.getValue());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasNext() {
        return it.hasNext();
    }
}
