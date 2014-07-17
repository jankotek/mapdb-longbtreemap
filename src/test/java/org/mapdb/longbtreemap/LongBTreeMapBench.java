package org.mapdb.longbtreemap;

import org.mapdb.*;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * Compares performance of `BTreeMap` and `LongBTreeMap`
 */
public class LongBTreeMapBench {

    static final long max = (long) 1e7;

    public static void main(String[] args) throws IOException {
        File f = File.createTempFile("mapdb","mapdb");

        //fill with data
        Iterator source = new Iterator(){

            long counter = max;
            @Override
            public boolean hasNext() {
                return counter>0;
            }

            @Override
            public Object next() {
                return counter--;
            }

            @Override
            public void remove() {

            }
        };

        DB db = DBMaker.newFileDB(f).transactionDisable().mmapFileEnable().make();

        Map map = db.createTreeMap("map")
                .pumpSource(source, Fun.extractNoTransform())
                .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
                .valueSerializer(Serializer.LONG)
                .make();
        db.commit();
        db.getEngine().clearCache();

        testMap(map);

        db = DBMaker.newFileDB(f).transactionDisable().mmapFileEnable().make();
        map = LongBTreeMapMaker.get(db,"map");

        testMap(map);

    }

    private static void testMap(Map map) {
        Random r = new Random(42);
        //randomly read vals
        long start = System.currentTimeMillis();
        long counter=0;
        while(start+10000>System.currentTimeMillis()){
            counter++;
            Long key = Math.abs(r.nextLong()%max);
            Long val = (Long) map.get(key);
        }
        System.out.printf("%s  %,d \n",map.getClass().getSimpleName(),counter);
    }
}
