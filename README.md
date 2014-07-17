This library offers BTreeMap which uses primitive `long` as key. 
It brings less memory overhead, better memory locality and better performance. 

To use it with maven:
```xml

    <repositories>
        <repository>
            <id>sonatype-snapshots</id>
            <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        </repository>
    </repositories>


    <dependencies>
        <dependency>
            <groupId>org.mapdb</groupId>
            <artifactId>mapdb-longbtreemap</artifactId>
            <version>0.1-SNAPSHOT</version>
        </dependency>
    </dependencies>
```

And instantiate using following code.

```java

    import org.mapdb.longbtreemap.*;
    
    DB db = DBMaker....;
    
    ConcurrentNavigableMap<Long,Long> map = LongBTreeMapMaker.get(db,"mapName");

```

Long BTreeMap has 100% compatible format with following BTreeMap:

```java

    db.createTreeMap("map")
        .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
        .valueSerializer(Serializer.LONG)
        .make();
        
```

So LongBTreeMap can be interchanged with BTreeMap, but MapDB store needs to be reopened before each swap

LongBTreeMap is about 1.8x faster compared to BTreeMap