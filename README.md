Pickle - Persistent key/value storage in Java
======

In Java, data structures such as Maps and Sets are very useful, but
when dealing with large amounts of data there are practical limits
on memory size.  Oftentimes you want to persist this data to disk
or database.  Key/value stores are ubiquitous and there are many
implemtations, but I haven't found any that implement the Map/Set
interfaces.  Pickle provides this capability with
implementations of Map and Set, using persistent key/value stores
as underlying engines.  Any Serializable objects can be used as
keys and values.

Current implementations:
- JDBM (http://jdbm.sourceforge.net/)
- Accumulo (http://accumulo.apache.org/)

Planned implementations:
- LevelDB (http://code.google.com/p/leveldb/)
- Cdb (http://cr.yp.to/cdb.html)
- Berkeley DB (http://www.oracle.com/technetwork/products/berkeleydb/)

Other potential implementations:
- SQLite (http://www.sqlite.org/)
- Voldemort (http://www.project-voldemort.com/)
- MapDB (http://www.mapdb.org/)
- ... (other suggestions welcome)

Planned features:
- PickleMap/PickleSet interfaces (closeable)
- Generic Pickle set leveraging Pickle map
- PickleException
- Using Kryo for serialization
- Compression
- More of the NavigableSet features
