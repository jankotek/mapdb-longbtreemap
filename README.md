This library offers BTreeMap which uses primitive `long` as key. 
It brings less memory overhead, better memory locality and better performance. 

TO use it with maven:
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