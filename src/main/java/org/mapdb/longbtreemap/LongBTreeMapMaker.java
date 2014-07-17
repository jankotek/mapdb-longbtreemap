package org.mapdb.longbtreemap;

import org.mapdb.*;


/**
 * Builder
 */
public class LongBTreeMapMaker {


    public static LongBTreeMap get(DB db, String name){
        synchronized (db) {
            db.checkNotClosed();
            LongBTreeMap ret = (LongBTreeMap) db.getFromWeakCollection(name);
            if (ret != null) return ret;
            String type = db.catGet(name + ".type", null);
            if (type == null) {
                db.checkShouldCreate(name);
                if (db.getEngine().isReadOnly()) {
                    Engine e = new StoreHeap();
                    new DB(e).getTreeMap("a");
                    return db.namedPut(name,
                            new DB(new EngineWrapper.ReadOnlyEngine(e)).getTreeMap("a"));
                }
                return create(db, name);

            }
            db.checkType(type, "TreeMap");


            if(db.catGet(name+".keySerializer",null)!=BTreeKeySerializer.ZERO_OR_POSITIVE_LONG){
                throw new IllegalArgumentException("TreeMap key serializer must be ZERO_OR_POSITIVE_LONG");
            }
            if(db.catGet(name+".valueSerializer",null)!=Serializer.LONG){
                throw new IllegalArgumentException("TreeMap value serializer must be ");
            }

            ret = new LongBTreeMap(db.getEngine(),
                    (Long) db.catGet(name + ".rootRecidRef"),
                    db.catGet(name + ".maxNodeSize", 32),
                    db.catGet(name + ".counterRecid", 0L),
                    db.catGet(name + ".numberOfNodeMetas", 0),
                    false
            );
            db.namedPut(name, ret);
            return ret;
        }

    }

    public static LongBTreeMap create(DB db, String name){
        return create(db,name,32,false);
    }

    public static LongBTreeMap create(DB db, String name, int maxNodeSize, boolean counterEnabled){
        synchronized (db) {
            db.checkNameNotExists(name);

            long counterRecid = counterEnabled ? 0L : db.getEngine().put(0L, Serializer.LONG);

            long rootRecidRef;
            rootRecidRef = LongBTreeMap.createRootRef(db.getEngine(), 0);

            db.catPut(name+".keySerializer",BTreeKeySerializer.ZERO_OR_POSITIVE_LONG);
            db.catPut(name+".valueSerializer",Serializer.LONG);

            LongBTreeMap ret = new LongBTreeMap(db.getEngine(),
                    db.catPut(name + ".rootRecidRef", rootRecidRef),
                    db.catPut(name + ".maxNodeSize", maxNodeSize),
                    db.catPut(name + ".counterRecid", counterRecid),
                    db.catPut(name + ".numberOfNodeMetas", 0),
                    false
            );
            db.catPut(name + ".type", "TreeMap");
            db.namedPut(name, ret);
            return ret;
        }
    }

}
