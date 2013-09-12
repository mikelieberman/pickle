Pickle - Persistent key/value storage in Java
======

In Java, data structures such as Maps and Sets are very useful, but
when dealing with large amounts of data there are practical limits on
memory size.  Oftentimes you want to persist this data to disk or
database.  Key/value stores are ubiquitous and there are many
implemtations, but I haven't found any that implement the Map/Set
interfaces.  I come from a Perl background and the lack of a simple
tie-like interface for hashes was driving me crazy.  To address this,
Pickle provides this capability with implementations of Map and Set,
using persistent key/value stores as underlying engines.  Any
Serializable objects can be used as keys and values; Kryo is used for
serialization.

Current implementations:
- Accumulo (http://accumulo.apache.org/)
- LevelDB (http://code.google.com/p/leveldb/)
- Cdb (http://cr.yp.to/cdb.html)
- SQLite (http://www.sqlite.org/)
- MapDB (http://www.mapdb.org/)

Planned implementations:
- Berkeley DB (http://www.oracle.com/technetwork/products/berkeleydb/)
- JDBM (http://jdbm.sourceforge.net/)

Other potential implementations:
- Voldemort (http://www.project-voldemort.com/)
- MongoDB (http://www.mongodb.org/)
- ... (other suggestions welcome)

Planned features:
- BTree implementation for navigable maps/sets
- Generic Pickle set leveraging Pickle map
- PickleException
- Compression

