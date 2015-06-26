# Sorted string table
Java implementation of immutable key-value storage based on sorted string table

# How to use
Building key-value storage:
```
TableBuilder builder = new TableBuilder();
builder.setUseBloomFilter(true);
builder.put("abc".getBytes(), "123".getBytes());
// Put some more data
builder.writeTo(new File("data.sst"));
```
Using key-value storage:
```
Table table = TableReader.getReader().from(new File("data.sst"));
byte[] value = table.get("abc".getBytes());
```
