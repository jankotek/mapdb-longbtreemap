package org.mapdb.longbtreemap;

import org.junit.Before;
import org.junit.Test;
import org.mapdb.*;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

import static org.junit.Assert.*;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class LongBTreeMapTest {

    Engine engine;


    LongBTreeMap m;

    @Before public void init(){
        engine = new StoreDirect(Volume.memoryFactory(false, 0L, CC.VOLUME_SLICE_SHIFT));
        m = new LongBTreeMap(engine, LongBTreeMap.createRootRef(engine, 0),
                6,0, 0,false);;
    }


    @Test public void test_leaf_node_serialization() throws IOException {


        LongBTreeMap.LeafNode n = new LongBTreeMap.LeafNode(new long[]{ LongBTreeMap.NULL,1L,2L,3L, LongBTreeMap.NULL}, new long[]{1L,2L,3L}, 111);
        LongBTreeMap.LeafNode n2 = (LongBTreeMap.LeafNode) clone(n, m.nodeSerializer);
        assertArrayEquals(n.keys(), n2.keys());
        assertEquals(n.next, n2.next);
    }


	@Test public void test_dir_node_serialization() throws IOException {


        LongBTreeMap.DirNode n = new LongBTreeMap.DirNode(new long[]{1L,2L,3L,  LongBTreeMap.NULL}, new long[]{4,5,6,7});
        LongBTreeMap.DirNode n2 = (LongBTreeMap.DirNode) clone(n, m.nodeSerializer);

        assertArrayEquals(n.keys(), n2.keys());
        assertArrayEquals(n.child, n2.child);
    }

    @Test public void test_find_children(){

        assertEquals(8,m.findChildren(11, new long[]{1,2,3,4,5,6,7,8}));
        assertEquals(0,m.findChildren(1, new long[]{1,2,3,4,5,6,7,8}));
        assertEquals(0,m.findChildren(0, new long[]{1,2,3,4,5,6,7,8}));
        assertEquals(7,m.findChildren(8, new long[]{1,2,3,4,5,6,7,8}));
        assertEquals(4,m.findChildren(49, new long[]{10,20,30,40,50}));
        assertEquals(4,m.findChildren(50, new long[]{10,20,30,40,50}));
        assertEquals(3,m.findChildren(40, new long[]{10,20,30,40,50}));
        assertEquals(3,m.findChildren(39, new long[]{10,20,30,40,50}));
    }


    @Test public void test_next_dir(){

        LongBTreeMap.DirNode d = new LongBTreeMap.DirNode(new long[]{44L,62L,68L, 71L}, new long[]{10,20,30,40});

        assertEquals(10, m.nextDir(d, 62L));
        assertEquals(10, m.nextDir(d, 44L));
        assertEquals(10, m.nextDir(d, 48L));

        assertEquals(20, m.nextDir(d, 63L));
        assertEquals(20, m.nextDir(d, 64L));
        assertEquals(20, m.nextDir(d, 68L));

        assertEquals(30, m.nextDir(d, 69L));
        assertEquals(30, m.nextDir(d, 70L));
        assertEquals(30, m.nextDir(d, 71L));

        assertEquals(40, m.nextDir(d, 72L));
        assertEquals(40, m.nextDir(d, 73L));
    }

    @Test public void test_next_dir_infinity(){

        LongBTreeMap.DirNode d = new LongBTreeMap.DirNode(
                new long[]{ LongBTreeMap.NULL,62L,68L, 71L},
                new long[]{10,20,30,40});
        assertEquals(10L, m.nextDir(d, 33L));
        assertEquals(10L, m.nextDir(d, 62L));
        assertEquals(20L, m.nextDir(d, 63L));

        d = new LongBTreeMap.DirNode(
                new long[]{44L,62L,68L,  LongBTreeMap.NULL},
                new long[]{10,20,30,40});

        assertEquals(10L, m.nextDir(d, 62L));
        assertEquals(10L, m.nextDir(d, 44L));
        assertEquals(10L, m.nextDir(d, 48L));

        assertEquals(20L, m.nextDir(d, 63L));
        assertEquals(20L, m.nextDir(d, 64L));
        assertEquals(20L, m.nextDir(d, 68L));

        assertEquals(30L, m.nextDir(d, 69L));
        assertEquals(30L, m.nextDir(d, 70L));
        assertEquals(30L, m.nextDir(d, 71L));

        assertEquals(30L, m.nextDir(d, 72L));
        assertEquals(30L, m.nextDir(d, 73L));

    }

    @Test public void simple_root_get(){

        LongBTreeMap.LeafNode l = new LongBTreeMap.LeafNode(
                new long[]{ LongBTreeMap.NULL, 10L,20L,30L,  LongBTreeMap.NULL},
                new long[]{10L,20L,30L},
                0);
        long rootRecid = engine.put(l, m.nodeSerializer);
        engine.update(m.rootRecidRef, rootRecid, Serializer.LONG);

        assertEquals(null, m.get(1L));
        assertEquals(null, m.get(9L));
        assertEquals(new Long(10L), m.get(10L));
        assertEquals(null, m.get(11L));
        assertEquals(null, m.get(19L));
        assertEquals(new Long(20L), m.get(20L));
        assertEquals(null, m.get(21L));
        assertEquals(null, m.get(29L));
        assertEquals(new Long(30L), m.get(30L));
        assertEquals(null, m.get(31L));
    }

    @Test public void root_leaf_insert(){

        m.put(11L,12L);
        final long rootRecid = engine.get(m.rootRecidRef, Serializer.LONG);
        LongBTreeMap.LeafNode n = (LongBTreeMap.LeafNode) engine.get(rootRecid, m.nodeSerializer);
        assertArrayEquals(new long[]{ LongBTreeMap.NULL, 11L,  LongBTreeMap.NULL}, n.keys);
        assertArrayEquals(new long[]{12L}, n.vals);
        assertEquals(0, n.next);
    }

    @Test public void batch_insert(){


        for(long i=0;i<1000;i++){
            m.put(i*10,i*10+1);
        }


        for(long i=0;i<10000;i++){
            assertEquals(i%10==0?i+1:null, m.get(i));
        }
    }

    @Test public void test_empty_iterator(){

        assertFalse(m.keySet().iterator().hasNext());
        assertFalse(m.values().iterator().hasNext());
    }

    @Test public void test_key_iterator(){

        for(long i = 0;i<20;i++){
            m.put(i,i*10);
        }

        Iterator iter = m.keySet().iterator();

        for(long i = 0;i<20;i++){
            assertTrue(iter.hasNext());
            assertEquals(i,iter.next());
        }
        assertFalse(iter.hasNext());
    }

    @Test public void test_size(){

        assertTrue(m.isEmpty());
        assertEquals(0,m.size());
        for(long i = 1;i<30;i++){
            m.put(i,i);
            assertEquals(i,m.size());
            assertFalse(m.isEmpty());
        }
    }

    @Test public void delete(){

        for(long i:new int[]{
                10, 50, 20, 42,
                //44, 68, 20, 93, 85, 71, 62, 77, 4, 37, 66
        }){
            m.put(i,i);
        }
        assertEquals(new Long(10L), m.remove(10L));
        assertEquals(new Long(20L), m.remove(20L));
        assertEquals(new Long(42L), m.remove(42L));

        assertEquals(null, m.remove(42999L));
    }

    @Test public void issue_38(){
        Map<Integer, String[]> map = DBMaker
                .newMemoryDB()
                .make().getTreeMap("test");

        for (int i = 0; i < 50000; i++) {
            map.put(i, new String[5]);

        }


        for (int i = 0; i < 50000; i=i+1000) {
            assertArrayEquals(new String[5], map.get(i));
            assertTrue(map.get(i).toString().contains("[Ljava.lang.String"));
        }


    }



    @Test public void floorTestFill() {

        m.put(1L, 1L);
        m.put(2L, 1L);
        m.put(5L, 1L);

        assertEquals(new Long(5L),m.floorKey(5L));
        assertEquals(new Long(1L),m.floorKey(1L));
        assertEquals(new Long(2L),m.floorKey(2L));
        assertEquals(new Long(2L),m.floorKey(3L));
        assertEquals(new Long(2L),m.floorKey(4L));
        assertEquals(new Long(5L),m.floorKey(5L));
        assertEquals(new Long(5L),m.floorKey(6L));
    }

    @Test public void submapToString() {


        for (long i = 0; i < 20; i++) {
            m.put(i, i);

        }

        Map submap = m.subMap(10L, true, 13L, true);
        assertEquals("{10=10, 11=11, 12=12, 13=13}",submap.toString());
    }

    @Test public void findSmaller(){


        for(long i=0;i<10000; i+=3){
            m.put(i, i);
        }

        for(long i=0;i<10000; i+=1){
            Long s = i - i%3;
            Map.Entry e = m.findSmaller(i,true);
            assertEquals(s,e!=null?e.getKey():null);
        }

        assertEquals(new Long(9999L), m.findSmaller(100000L,true).getKey());

        assertNull(m.findSmaller(0L,false));
        for(long i=1;i<10000; i+=1){
            Long s = i - i%3;
            if(s==i) s-=3;
            Map.Entry e = m.findSmaller(i,false);
            assertEquals(s,e!=null?e.getKey():null);
        }
        assertEquals(new Long(9999), m.findSmaller(100000L,false).getKey());

    }


    @Test public void concurrent_last_key(){
        DB db = DBMaker.newMemoryDB().make();
        final LongBTreeMap m = LongBTreeMapMaker.get(db,"map");

        //fill
        final long c = 1000000;
        for(long i=0;i<=c;i++){
            m.put(i,i);
        }

        Thread t = new Thread(){
            @Override
            public void run() {
                for(long i=c;i>=0;i--){
                    m.remove(i);
                }
            }
        };
        t.run();
        while(t.isAlive()){
            assertNotNull(m.lastKey());
        }
    }

    @Test public void concurrent_first_key(){
        DB db = DBMaker.newMemoryDB().make();
        final LongBTreeMap m = LongBTreeMapMaker.get(db,"map");

        //fill
        final long c = 1000000;
        for(long i=0;i<=c;i++){
            m.put(i,i);
        }

        Thread t = new Thread(){
            @Override
            public void run() {
                for(long i=0;i<=c;i++){
                    m.remove(c);
                }
            }
        };
        t.run();
        while(t.isAlive()){
            assertNotNull(m.firstKey());
        }
    }

    @Test public void WriteDBInt_lastKey() {
        long numberOfRecords = 1000;

        /** Creates connections to MapDB */
        DB db1 = DBMaker.newMemoryDB().make();


        /** Creates maps */
        ConcurrentNavigableMap<Long, Long> map1 = LongBTreeMapMaker.get(db1,"map");

        /** Inserts initial values in maps */
        for (long i = 0; i < numberOfRecords; i++) {
            map1.put(i, i);
        }


        assertEquals((Object) (numberOfRecords - 1), map1.lastKey());

        map1.clear();

        /** Inserts some values in maps */
        for (long i = 0; i < 10; i++) {
            map1.put(i, i);
        }

        assertEquals(10,map1.size());
        assertFalse(map1.isEmpty());
        assertEquals((Object) 9L, map1.lastKey());
        assertEquals((Object) 9L, map1.lastEntry().getValue());
        assertEquals((Object) 0L, map1.firstKey());
        assertEquals((Object) 0L, map1.firstEntry().getValue());
    }


    @Test public void WriteDBInt_lastKey_middle() {
        long numberOfRecords = 1000;

        /** Creates connections to MapDB */
        DB db1 = DBMaker.newMemoryDB().make();


        /** Creates maps */
        final LongBTreeMap map1 = LongBTreeMapMaker.get(db1,"map");

        /** Inserts initial values in maps */
        for (long i = 0; i < numberOfRecords; i++) {
            map1.put(i, i);
        }


        assertEquals((Object) (numberOfRecords - 1), map1.lastKey());

        map1.clear();

        /** Inserts some values in maps */
        for (long i = 100; i < 110; i++) {
            map1.put(i, i);
        }

        assertEquals(10,map1.size());
        assertFalse(map1.isEmpty());
        assertEquals((Object) 109L, map1.lastKey());
        assertEquals((Object) 109L, map1.lastEntry().getValue());
        assertEquals((Object) 100L, map1.firstKey());
        assertEquals((Object) 100L, map1.firstEntry().getValue());
    }


    /** clone value using serialization */
    public static <E> E clone(E value, Serializer<E> serializer){
        try{
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out,value);
            DataInput2 in = new DataInput2(ByteBuffer.wrap(out.copyBytes()), 0);

            return serializer.deserialize(in,out.pos);
        }catch(IOException ee){
            throw new IOError(ee);
        }
    }

    @Test public void reopen_treeMap() throws IOException {
        File f = File.createTempFile("mapdb","mapdb");
        DB db = DBMaker.newFileDB(f).transactionDisable().make();
        Map m = db.createTreeMap("map")
                .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
                .valueSerializer(Serializer.LONG)
                .make();
        m.put(11L,22L);

        db.close();
        db = DBMaker.newFileDB(f).transactionDisable().make();
        m = LongBTreeMapMaker.get(db,"map");
        assertEquals(22L, m.get(11L));
        db.close();
    }


    @Test public void reopen_treeMap2() throws IOException {
        File f = File.createTempFile("mapdb","mapdb");
        DB db = DBMaker.newFileDB(f).transactionDisable().make();
        Map m = LongBTreeMapMaker.get(db,"map");
        m.put(11L,22L);

        db.close();
        db = DBMaker.newFileDB(f).transactionDisable().make();
        m = db.getTreeMap("map");
        assertEquals(22L, m.get(11L));
        db.close();
    }


}



