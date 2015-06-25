# Sorted string table
Java implementation of immutable key-value storage based on sorted string table

# How to use
Building key-value storage:
```
SSTableBuilder builder = new SSTableBuilder();
builder.setUseBloomFilter(true);
builder.put("abc".getBytes(), "123".getBytes());
// Put some more data
builder.writeTo(new File("data.sst"));
```
Using key-value storage:
```
SSTable table = SSTableReader.getReader().from(new File("data.sst"));
byte[] value = table.get("abc".getBytes());
```
