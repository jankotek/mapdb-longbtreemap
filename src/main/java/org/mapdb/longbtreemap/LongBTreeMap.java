/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * NOTE: some code (and javadoc) used in this class
 * comes from JSR-166 group with following copyright:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.mapdb.longbtreemap;


import org.mapdb.*;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;


/**
 * A scalable concurrent {@link java.util.concurrent.ConcurrentNavigableMap} implementation.
 * The map is sorted according to the {@linkplain Comparable natural
 * ordering} of its keys, or by a {@link java.util.Comparator} provided at map
 * creation time.
 *
 * Insertion, removal,
 * update, and access operations safely execute concurrently by
 * multiple threads.  Iterators are <i>weakly consistent</i>, returning
 * elements reflecting the state of the map at some point at or since
 * the creation of the iterator.  They do <em>not</em> throw {@link
 * java.util.ConcurrentModificationException}, and may proceed concurrently with
 * other operations. Ascending key ordered views and their iterators
 * are faster than descending ones.
 *
 * It is possible to obtain <i>consistent</i> iterator by using <code>snapshot()</code>
 * method.
 *
 * All <tt>Map.Entry</tt> pairs returned by methods in this class
 * and its views represent snapshots of mappings at the time they were
 * produced. They do <em>not</em> support the <tt>Entry.setValue</tt>
 * method. (Note however that it is possible to change mappings in the
 * associated map using <tt>put</tt>, <tt>putIfAbsent</tt>, or
 * <tt>replace</tt>, depending on exactly which effect you need.)
 *
 * This collection has optional size counter. If this is enabled Map size is
 * kept in {@link org.mapdb.Atomic.Long} variable. Keeping counter brings considerable
 * overhead on inserts and removals.
 * If the size counter is not enabled the <tt>size</tt> method is <em>not</em> a constant-time operation.
 * Determining the current number of elements requires a traversal of the elements.
 *
 * Additionally, the bulk operations <tt>putAll</tt>, <tt>equals</tt>, and
 * <tt>clear</tt> are <em>not</em> guaranteed to be performed
 * atomically. For example, an iterator operating concurrently with a
 * <tt>putAll</tt> operation might view only some of the added
 * elements. NOTE: there is an optional
 *
 * This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link java.util.Map} and {@link java.util.Iterator}
 * interfaces. Like most other concurrent collections, this class does
 * <em>not</em> permit the use of <tt>null</tt> keys or values because some
 * null return values cannot be reliably distinguished from the absence of
 * elements.
 *
 * Theoretical design of BTreeMap is based on <a href="http://www.cs.cornell.edu/courses/cs4411/2009sp/blink.pdf">paper</a>
 * from Philip L. Lehman and S. Bing Yao. More practical aspects of BTreeMap implementation are based on <a href="http://www.doc.ic.ac.uk/~td202/">notes</a>
 * and <a href="http://www.doc.ic.ac.uk/~td202/btree/">demo application</a> from Thomas Dinsdale-Young.
 * B-Linked-Tree used here does not require locking for read. Updates and inserts locks only one, two or three nodes.

 * This B-Linked-Tree structure does not support removal well, entry deletion does not collapse tree nodes. Massive
 * deletion causes empty nodes and performance lost. There is workaround in form of compaction process, but it is not
 * implemented yet.
 *
 * @author Jan Kotek
 * @author some parts by Doug Lea and JSR-166 group
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class LongBTreeMap extends AbstractMap<Long, Long>
        implements ConcurrentNavigableMap<Long, Long>, Bind.MapWithModificationListener<Long, Long>, Closeable {

    @SuppressWarnings("rawtypes")
    public static final Comparator COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };


    protected static final int B_TREE_NODE_LEAF_LR = 180;
    protected static final int B_TREE_NODE_LEAF_L = 181;
    protected static final int B_TREE_NODE_LEAF_R = 182;
    protected static final int B_TREE_NODE_LEAF_C = 183;
    protected static final int B_TREE_NODE_DIR_LR = 184;
    protected static final int B_TREE_NODE_DIR_L = 185;
    protected static final int B_TREE_NODE_DIR_R = 186;
    protected static final int B_TREE_NODE_DIR_C = 187;



    /** recid under which reference to rootRecid is stored */
    protected final long rootRecidRef;

    /** holds node level locks*/
    protected final LongConcurrentHashMap<Thread> nodeLocks = new LongConcurrentHashMap<Thread>();

    /** maximal node size allowed in this BTree*/
    protected final int maxNodeSize;

    /** DB Engine in which entries are persisted */
    protected final Engine engine;

    protected final List<Long> leftEdges;


    private final KeySet keySet;

    private final EntrySet entrySet = new EntrySet(this);

    private final Values values = new Values(this);

    private final ConcurrentNavigableMap<Long, Long> descendingMap = new DescendingMap(this, null,true, null, false);

    protected final Atomic.Long counter;

    protected final int numberOfNodeMetas;




    /** common interface for BTree node */
    protected interface BNode{
        boolean isLeaf();
        Long[] keys();
        Long[] vals();
        Long highKey();
        long[] child();
        long next();
    }

    protected final static class DirNode implements BNode{
        final Long[] keys;
        final long[] child;

        DirNode(Long[] keys, long[] child) {
            this.keys = keys;
            this.child = child;
        }

        DirNode(Long[] keys, List<Long> child) {
            this.keys = keys;
            this.child = new long[child.size()];
            for(int i=0;i<child.size();i++){
                this.child[i] = child.get(i);
            }
        }


        @Override
        public boolean isLeaf() { return false;}

        @Override
        public Long[] keys() { return keys;}
        @Override
        public Long[] vals() { return null;}

        @Override
        public Long highKey() {return keys[keys.length-1];}

        @Override
        public long[] child() { return child;}

        @Override
        public long next() {return child[child.length-1];}

        @Override
        public String toString(){
            return "Dir(K"+ Arrays.toString(keys)+", C"+ Arrays.toString(child)+")";
        }

    }


    protected final static class LeafNode implements BNode{
        final Long[] keys;
        final Long[] vals;
        final long next;

        LeafNode(Long[] keys, Long[] vals, long next) {
            this.keys = keys;
            this.vals = vals;
            this.next = next;
            assert(vals==null||keys.length == vals.length+2);
        }

        @Override
        public boolean isLeaf() { return true;}

        @Override
        public Long[] keys() { return keys;}
        @Override
        public Long[] vals() { return vals;}

        @Override
        public Long highKey() {return keys[keys.length-1];}

        @Override
        public long[] child() { return null;}
        @Override
        public long next() {return next;}

        @Override
        public String toString(){
            return "Leaf(K"+ Arrays.toString(keys)+", V"+ Arrays.toString(vals)+", L="+next+")";
        }
    }


    protected final Serializer<BNode> nodeSerializer;

    protected static class NodeSerializer<A,B> implements Serializer<BNode> {

        protected final int numberOfNodeMetas;

        public NodeSerializer(int numberOfNodeMetas) {
            this.numberOfNodeMetas = numberOfNodeMetas;
        }

        @Override
        public void serialize(DataOutput out, BNode value) throws IOException {
            final boolean isLeaf = value.isLeaf();

            //first byte encodes if is leaf (first bite) and length (last seven bites)
            assert(value.keys().length<=255);
            assert(!(!isLeaf && value.child().length!= value.keys().length));
            assert(!(!isLeaf && value.highKey()!=null && value.child()[value.child().length-1]==0));

            //check node integrity in paranoid mode
            if(CC.PARANOID){
                int len = value.keys().length;
                for(int i=value.keys()[0]==null?2:1;
                  i<(value.keys()[len-1]==null?len-1:len);
                  i++){
                    int comp = Fun.COMPARATOR.compare(value.keys()[i-1], value.keys()[i]);
                    int limit = i==len-1 ? 1:0 ;
                    if(comp>=limit){
                        throw new AssertionError("BTreeNode format error, wrong key order at #"+i+"\n"+value);
                    }
                }

            }


            final boolean left = value.keys()[0] == null;
            final boolean right = value.keys()[value.keys().length-1] == null;


            final int header;

            if(isLeaf){
                if(right){
                    if(left)
                        header = B_TREE_NODE_LEAF_LR;
                    else
                        header = B_TREE_NODE_LEAF_R;
                }else{
                    if(left)
                        header = B_TREE_NODE_LEAF_L;
                    else
                        header = B_TREE_NODE_LEAF_C;
                }
            }else{
                if(right){
                    if(left)
                        header = B_TREE_NODE_DIR_LR;
                    else
                        header = B_TREE_NODE_DIR_R;
                }else{
                    if(left)
                        header = B_TREE_NODE_DIR_L;
                    else
                        header = B_TREE_NODE_DIR_C;
                }
            }



            out.write(header);
            out.write(value.keys().length);

            //write node metas, right now this is ignored, but in future it could be used for counted btrees or aggregations
            for(int i=0;i<numberOfNodeMetas;i++){
                DataOutput2.packLong(out, 0);
            }

            //longs go first, so it is possible to reconstruct tree without serializer
            if(isLeaf){
                DataOutput2.packLong(out, ((LeafNode) value).next);
            }else{
                for(long child : ((DirNode)value).child)
                    DataOutput2.packLong(out, child);
            }


            longSerialize(out,left?1:0,
                    right?value.keys().length-1:value.keys().length,
                    value.keys());

            if(isLeaf){

                    for(Long val:value.vals()){
                        assert(val!=null);
                        out.writeLong(val);
                    }

            }
        }

        @Override
        public BNode deserialize(DataInput in, int available) throws IOException {
            final int header = in.readUnsignedByte();
            final int size = in.readUnsignedByte();

            //read node metas, right now this is ignored, but in future it could be used for counted btrees or aggregations
            for(int i=0;i<numberOfNodeMetas;i++){
                DataInput2.unpackLong(in);
            }


            //first bite indicates leaf
            final boolean isLeaf =
                    header == B_TREE_NODE_LEAF_C  || header == B_TREE_NODE_LEAF_L ||
                    header == B_TREE_NODE_LEAF_LR || header == B_TREE_NODE_LEAF_R;
            final int start =
                (header==B_TREE_NODE_LEAF_L  || header == B_TREE_NODE_LEAF_LR || header==B_TREE_NODE_DIR_L  || header == B_TREE_NODE_DIR_LR) ?
                1:0;

            final int end =
                (header==B_TREE_NODE_LEAF_R  || header == B_TREE_NODE_LEAF_LR || header==B_TREE_NODE_DIR_R  || header == B_TREE_NODE_DIR_LR) ?
                size-1:size;


            if(isLeaf){
                long next = DataInput2.unpackLong(in);
                Long[] keys = longDeserialize(in, start,end,size);
                assert(keys.length==size);
                Long[] vals = new Long[size-2];

                for(int i=0;i<size-2;i++){
                    vals[i] = in.readLong();
                 }

                return new LeafNode(keys, vals, next);
            }else{
                long[] child = new long[size];
                for(int i=0;i<size;i++)
                    child[i] = DataInput2.unpackLong(in);
                Long[] keys = longDeserialize(in, start,end,size);
                assert(keys.length==size);
                return new DirNode(keys, child);
            }
        }

        @Override
        public int fixedSize() {
            return -1;
        }

        public void longSerialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
            if(start>=end) return;
            long prev = (Long)keys[start];
            DataOutput2.packLong(out,prev);
            for(int i=start+1;i<end;i++){
                long curr = (Long)keys[i];
                DataOutput2.packLong(out, curr-prev);
                prev = curr;
            }
        }

        public Long[] longDeserialize(DataInput in, int start, int end, int size) throws IOException {
            Long[] ret = new Long[size];
            long prev = 0 ;
            for(int i = start; i<end; i++){
                ret[i] = prev = prev + DataInput2.unpackLong(in);
            }
            return ret;
        }


    }


    /** Constructor used to create new BTreeMap.
     *
     * @param engine used for persistence
     * @param rootRecidRef reference to root recid
     * @param maxNodeSize maximal BTree Node size. Node will split if number of entries is higher
     * @param counterRecid recid under which `Atomic.Long` is stored, or `0` for no counter
     * @param numberOfNodeMetas number of meta records associated with each BTree node
     * @param disableLocks makes class thread-unsafe but bit faster
     */
    public LongBTreeMap(Engine engine, long rootRecidRef, int maxNodeSize,long counterRecid,
                        int numberOfNodeMetas, boolean disableLocks) {
        if(maxNodeSize%2!=0) throw new IllegalArgumentException("maxNodeSize must be dividable by 2");
        if(maxNodeSize<6) throw new IllegalArgumentException("maxNodeSize too low");
        if(maxNodeSize>126) throw new IllegalArgumentException("maxNodeSize too high");
        if(rootRecidRef<=0||counterRecid<0 || numberOfNodeMetas<0) throw new IllegalArgumentException();



        this.rootRecidRef = rootRecidRef;
        this.engine = engine;
        this.maxNodeSize = maxNodeSize;
        this.numberOfNodeMetas = numberOfNodeMetas;



        this.nodeSerializer = new NodeSerializer(numberOfNodeMetas);

        this.keySet = new KeySet(this);


        if(counterRecid!=0){
            this.counter = new Atomic.Long(engine,counterRecid);
            Bind.size(this, counter);
        }else{
            this.counter = null;
        }

        //load left edge refs
        ArrayList leftEdges2 = new ArrayList<Long>();
        long r = engine.get(rootRecidRef, Serializer.LONG);
        for(;;){
            BNode n= engine.get(r,nodeSerializer);
            leftEdges2.add(r);
            if(n.isLeaf()) break;
            r = n.child()[0];
        }
        Collections.reverse(leftEdges2);
        leftEdges = new CopyOnWriteArrayList<Long>(leftEdges2);
    }

    /** creates empty root node and returns recid of its reference*/
    static protected long createRootRef(Engine engine,int numberOfNodeMetas){
        final LeafNode emptyRoot = new LeafNode(new Long[]{null, null}, new Long[]{}, 0);
        //empty root is serializer simpler way, so we can use dummy values
        long rootRecidVal = engine.put(emptyRoot,  new NodeSerializer( numberOfNodeMetas));
        return engine.put(rootRecidVal, Serializer.LONG);
    }



    /**
     * Find the first children node with a key equal or greater than the given key.
     * If all items are smaller it returns `keys.length`
     */
    protected final int findChildren(final Object key, final Object[] keys) {
        int left = 0;
        if(keys[0] == null) left++;
        int right = keys[keys.length-1] == null ? keys.length-1 :  keys.length;

        int middle;

        // binary search
        while (true) {
            middle = (left + right) / 2;
            if(keys[middle]==null) return middle; //null is positive infinitive
            if (Fun.COMPARATOR.compare(keys[middle], key) < 0) {
                left = middle + 1;
            } else {
                right = middle;
            }
            if (left >= right) {
                return  right;
            }
        }

    }

    @Override
	public Long get(Object key){
    	return (Long) get(key, true);
    }

    protected Object get(Object key, boolean expandValue) {
        if(key==null) throw new NullPointerException();
        Long v = (Long) key;
        long current = engine.get(rootRecidRef, Serializer.LONG); //get root

        BNode A = engine.get(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, v);
            A = engine.get(current, nodeSerializer);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        int pos = findChildren(v, leaf.keys);
        while(pos == leaf.keys.length){
            //follow next link on leaf until necessary
            leaf = (LeafNode) engine.get(leaf.next, nodeSerializer);
            pos = findChildren(v, leaf.keys);
        }

        if(pos==leaf.keys.length-1){
            return null; //last key is always deleted
        }
        //finish search
        if(leaf.keys[pos]!=null && 0==Fun.COMPARATOR.compare(v,leaf.keys[pos])){
            Object ret = leaf.vals[pos-1];
            return expandValue ? (ret) : ret;
        }else
            return null;
    }

    protected long nextDir(DirNode d, Long key) {
        int pos = findChildren(key, d.keys) - 1;
        if(pos<0) pos = 0;
        return d.child[pos];
    }


    @Override
    public Long put(Long key, Long value){
        if(key==null||value==null) throw new NullPointerException();
        return put2(key,value, false);
    }

    protected Long put2(final Long key, final Long value2, final boolean putOnlyIfAbsent){
        Long v = key;
        if(v == null) throw new IllegalArgumentException("null key");
        if(value2 == null) throw new IllegalArgumentException("null value");

        Long value = value2;

        int stackPos = -1;
        long[] stackVals = new long[4];

        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        long current = rootRecid;

        BNode A = engine.get(current, nodeSerializer);
        while(!A.isLeaf()){
            long t = current;
            current = nextDir((DirNode) A, v);
            assert(current>0) : A;
            if(current == A.child()[A.child().length-1]){
                //is link, do nothing
            }else{
                //stack push t
                stackPos++;
                if(stackVals.length == stackPos) //grow if needed
                    stackVals = Arrays.copyOf(stackVals, stackVals.length * 2);
                stackVals[stackPos] = t;
            }
            A = engine.get(current, nodeSerializer);
        }
        int level = 1;

        long p=0;
        try{
        while(true){
            boolean found;
            do{
                lock(nodeLocks, current);
                found = true;
                A = engine.get(current, nodeSerializer);
                int pos = findChildren(v, A.keys());
                //check if keys is already in tree
                if(pos<A.keys().length-1 &&  v!=null && A.keys()[pos]!=null &&
                        0==Fun.COMPARATOR.compare(v,A.keys()[pos])){
                    //yes key is already in tree
                    Long oldVal = A.vals()[pos-1];
                    if(putOnlyIfAbsent){
                        //is not absent, so quit
                        unlock(nodeLocks, current);
                        if(CC.PARANOID) assertNoLocks(nodeLocks);
                        return (oldVal);
                    }
                    //insert new
                    Long[] vals = Arrays.copyOf(A.vals(), A.vals().length);
                    vals[pos-1] = value;

                    A = new LeafNode(Arrays.copyOf(A.keys(), A.keys().length), vals, ((LeafNode)A).next);
                    assert(nodeLocks.get(current)== Thread.currentThread());
                    engine.update(current, A, nodeSerializer);
                    //already in here
                    Long ret =  (oldVal);
                    notify(key,ret, value2);
                    unlock(nodeLocks, current);
                    if(CC.PARANOID) assertNoLocks(nodeLocks);
                    return ret;
                }

                //if v > highvalue(a)
                if(A.highKey() != null && Fun.COMPARATOR.compare(v, A.highKey())>0){
                    //follow link until necessary
                    unlock(nodeLocks, current);
                    found = false;
                    int pos2 = findChildren(v, A.keys());
                    while(A!=null && pos2 == A.keys().length){
                        //TODO lock?
                        long next = A.next();

                        if(next==0) break;
                        current = next;
                        A = engine.get(current, nodeSerializer);
                        pos2 = findChildren(v, A.keys());
                    }

                }


            }while(!found);

            // can be new item inserted into A without splitting it?
            if(A.keys().length - (A.isLeaf()?2:1)<maxNodeSize){
                int pos = findChildren(v, A.keys());
                Long[] keys = arrayPut(A.keys(), pos, v);

                if(A.isLeaf()){
                    Long[] vals = arrayPut(A.vals(), pos-1, value);
                    LeafNode n = new LeafNode(keys, vals, ((LeafNode)A).next);
                    assert(nodeLocks.get(current)== Thread.currentThread());
                    engine.update(current, n, nodeSerializer);
                }else{
                    assert(p!=0);
                    long[] child = arrayLongPut(A.child(), pos, p);
                    DirNode d = new DirNode(keys, child);
                    assert(nodeLocks.get(current)== Thread.currentThread());
                    engine.update(current, d, nodeSerializer);
                }

                notify(key,  null, value2);
                unlock(nodeLocks, current);
                if(CC.PARANOID) assertNoLocks(nodeLocks);
                return null;
            }else{
                //node is not safe, it requires splitting
                final int pos = findChildren(v, A.keys());
                final Long[] keys = arrayPut(A.keys(), pos, v);
                final Long[] vals = (A.isLeaf())? arrayPut(A.vals(), pos-1, value) : null;
                final long[] child = A.isLeaf()? null : arrayLongPut(A.child(), pos, p);
                final int splitPos = keys.length/2;
                BNode B;
                if(A.isLeaf()){
                    Long[] vals2 = Arrays.copyOfRange(vals, splitPos, vals.length);

                    B = new LeafNode(
                                Arrays.copyOfRange(keys, splitPos, keys.length),
                                vals2,
                                ((LeafNode)A).next);
                }else{
                    B = new DirNode(Arrays.copyOfRange(keys, splitPos, keys.length),
                                Arrays.copyOfRange(child, splitPos, keys.length));
                }
                long q = engine.put(B, nodeSerializer);
                if(A.isLeaf()){  //  splitPos+1 is there so A gets new high  value (key)
                    Long[] keys2 = Arrays.copyOf(keys, splitPos + 2);
                    keys2[keys2.length-1] = keys2[keys2.length-2];
                    Long[] vals2 = Arrays.copyOf(vals, splitPos);

                    //TODO check high/low keys overlap
                    A = new LeafNode(keys2, vals2, q);
                }else{
                    long[] child2 = Arrays.copyOf(child, splitPos + 1);
                    child2[splitPos] = q;
                    A = new DirNode(Arrays.copyOf(keys, splitPos + 1), child2);
                }
                assert(nodeLocks.get(current)== Thread.currentThread());
                engine.update(current, A, nodeSerializer);

                if((current != rootRecid)){ //is not root
                    unlock(nodeLocks, current);
                    p = q;
                    v = (Long) A.highKey();
                    level = level+1;
                    if(stackPos!=-1){ //if stack is not empty
                        current = stackVals[stackPos--];
                    }else{
                        //current := the left most node at level
                        current = leftEdges.get(level-1);
                      }
                    assert(current>0);
                }else{
                    BNode R = new DirNode(
                            new Long[]{A.keys()[0], A.highKey(), B.isLeaf()?null:B.highKey()},
                            new long[]{current,q, 0});

                    lock(nodeLocks, rootRecidRef);
                    unlock(nodeLocks, current);
                    long newRootRecid = engine.put(R, nodeSerializer);

                    assert(nodeLocks.get(rootRecidRef)== Thread.currentThread());
                    engine.update(rootRecidRef, newRootRecid, Serializer.LONG);
                    //add newRootRecid into leftEdges
                    leftEdges.add(newRootRecid);

                    notify(key, null, value2);
                    unlock(nodeLocks, rootRecidRef);
                    if(CC.PARANOID) assertNoLocks(nodeLocks);
                    return null;
                }
            }
        }
        }catch(RuntimeException e){
            unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            unlockAll(nodeLocks);
            throw new RuntimeException(e);
        }
    }


    protected static class BTreeIterator{
        final LongBTreeMap m;

        LeafNode currentLeaf;
        Object lastReturnedKey;
        int currentPos;
        final Object hi;
        final boolean hiInclusive;

        /** unbounded iterator*/
        BTreeIterator(LongBTreeMap m){
            this.m = m;
            hi=null;
            hiInclusive=false;
            pointToStart();
        }

        /** bounder iterator, args may be null for partially bounded*/
        BTreeIterator(LongBTreeMap m, Long lo, boolean loInclusive, Long hi, boolean hiInclusive){
            this.m = m;
            if(lo==null){
                pointToStart();
            }else{
                Fun.Tuple2<Integer, LeafNode> l = m.findLargerNode(lo, loInclusive);
                currentPos = l!=null? l.a : -1;
                currentLeaf = l!=null ? l.b : null;
            }

            this.hi = hi;
            this.hiInclusive = hiInclusive;
            if(hi!=null && currentLeaf!=null){
                //check in bounds
                Object key =  currentLeaf.keys[currentPos];
                int c = Fun.COMPARATOR.compare(key, hi);
                if (c > 0 || (c == 0 && !hiInclusive)){
                    //out of high bound
                    currentLeaf=null;
                    currentPos=-1;
                }
            }

        }


        private void pointToStart() {
            //find left-most leaf
            final long rootRecid = m.engine.get(m.rootRecidRef, Serializer.LONG);
            BNode node = (BNode) m.engine.get(rootRecid, m.nodeSerializer);
            while(!node.isLeaf()){
                node = (BNode) m.engine.get(node.child()[0], m.nodeSerializer);
            }
            currentLeaf = (LeafNode) node;
            currentPos = 1;

            while(currentLeaf.keys.length==2){
                //follow link until leaf is not empty
                if(currentLeaf.next == 0){
                    currentLeaf = null;
                    return;
                }
                currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
            }
        }


        public boolean hasNext(){
            return currentLeaf!=null;
        }

        public void remove(){
            if(lastReturnedKey==null) throw new IllegalStateException();
            m.remove(lastReturnedKey);
            lastReturnedKey = null;
        }

        protected void advance(){
            if(currentLeaf==null) return;
            lastReturnedKey =  currentLeaf.keys[currentPos];
            currentPos++;
            if(currentPos == currentLeaf.keys.length-1){
                //move to next leaf
                if(currentLeaf.next==0){
                    currentLeaf = null;
                    currentPos=-1;
                    return;
                }
                currentPos = 1;
                currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
                while(currentLeaf.keys.length==2){
                    if(currentLeaf.next ==0){
                        currentLeaf = null;
                        currentPos=-1;
                        return;
                    }
                    currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
                }
            }
            if(hi!=null && currentLeaf!=null){
                //check in bounds
                Object key =  currentLeaf.keys[currentPos];
                int c = Fun.COMPARATOR.compare(key, hi);
                if (c > 0 || (c == 0 && !hiInclusive)){
                    //out of high bound
                    currentLeaf=null;
                    currentPos=-1;
                }
            }
        }
    }

    @Override
	public Long remove(Object key) {
        return remove2((Long)key, null);
    }

    private Long remove2(final Long key, final Long value) {
        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode A = engine.get(current, nodeSerializer);
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, key);
            A = engine.get(current, nodeSerializer);
        }

        try{
        while(true){

            lock(nodeLocks, current);
            A = engine.get(current, nodeSerializer);
            int pos = findChildren(key, A.keys());
            if(pos<A.keys().length&& key!=null && A.keys()[pos]!=null &&
                    0==Fun.COMPARATOR.compare(key,A.keys()[pos])){
                //check for last node which was already deleted
                if(pos == A.keys().length-1 && value == null){
                    unlock(nodeLocks, current);
                    return null;
                }

                //delete from node
                Object oldVal =   A.vals()[pos-1];

                if(value!=null && !value.equals(oldVal)){
                    unlock(nodeLocks, current);
                    return null;
                }

                Long[] keys2 = new Long[A.keys().length-1];
                System.arraycopy(A.keys(), 0, keys2, 0, pos);
                System.arraycopy(A.keys(), pos + 1, keys2, pos, keys2.length - pos);

                Long[] vals2 = new Long[A.vals().length-1];
                System.arraycopy(A.vals(), 0, vals2, 0, pos - 1);
                System.arraycopy(A.vals(), pos, vals2, pos - 1, vals2.length - (pos - 1));


                A = new LeafNode(keys2, vals2, ((LeafNode)A).next);
                assert(nodeLocks.get(current)== Thread.currentThread());
                engine.update(current, A, nodeSerializer);
                notify((Long)key, (Long)oldVal, null);
                unlock(nodeLocks, current);
                return (Long) oldVal;
            }else{
                unlock(nodeLocks, current);
                //follow link until necessary
                if(A.highKey() != null && Fun.COMPARATOR.compare(key, A.highKey())>0){
                    int pos2 = findChildren(key, A.keys());
                    while(pos2 == A.keys().length){
                        //TODO lock?
                        current = ((LeafNode)A).next;
                        A = engine.get(current, nodeSerializer);
                    }
                }else{
                    return null;
                }
            }
        }
        }catch(RuntimeException e){
            unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            unlockAll(nodeLocks);
            throw new RuntimeException(e);
        }
    }


    @Override
    public void clear() {
        Iterator iter = keyIterator();
        while(iter.hasNext()){
            iter.next();
            iter.remove();
        }
    }


    static class BTreeKeyIterator<K> extends BTreeIterator implements Iterator<K> {

        BTreeKeyIterator(LongBTreeMap m) {
            super(m);
        }

        BTreeKeyIterator(LongBTreeMap m, Long lo, boolean loInclusive, Long hi, boolean hiInclusive) {
            super(m, lo, loInclusive, hi, hiInclusive);
        }

        @Override
        public K next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            K ret = (K) currentLeaf.keys[currentPos];
            advance();
            return ret;
        }
    }

    static  class BTreeValueIterator extends BTreeIterator implements Iterator<Long> {

        BTreeValueIterator(LongBTreeMap m) {
            super(m);
        }

        BTreeValueIterator(LongBTreeMap m, Long lo, boolean loInclusive, Long hi, boolean hiInclusive) {
            super(m, lo, loInclusive, hi, hiInclusive);
        }

        @Override
        public Long next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            Long ret = currentLeaf.vals[currentPos-1];
            advance();
            return ret;
        }

    }

    static  class BTrLongntryIterator extends BTreeIterator implements Iterator<Entry<Long, Long>> {

        BTrLongntryIterator(LongBTreeMap m) {
            super(m);
        }

        BTrLongntryIterator(LongBTreeMap m, Long lo, boolean loInclusive, Long hi, boolean hiInclusive) {
            super(m, lo, loInclusive, hi, hiInclusive);
        }

        @Override
        public Entry<Long, Long> next() {
            if(currentLeaf == null) throw new NoSuchElementException();
            Long ret =  currentLeaf.keys[currentPos];
            Long val = currentLeaf.vals[currentPos-1];
            advance();
            return m.makeEntry(ret, val);

        }
    }






    protected Entry<Long, Long> makeEntry(Object key, Object value) {
        return new SimpleImmutableEntry<Long, Long>((Long)key,  (Long)value);
    }


    @Override
    public boolean isEmpty() {
        return !keyIterator().hasNext();
    }

    @Override
    public int size() {
        long size = sizeLong();
        if(size> Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return (int) size;
    }

    @Override
    public long sizeLong() {
        if(counter!=null)
            return counter.get();

        long size = 0;
        BTreeIterator iter = new BTreeIterator(this);
        while(iter.hasNext()){
            iter.advance();
            size++;
        }
        return size;
    }

    @Override
    public Long putIfAbsent(Long key, Long value) {
        if(key == null || value == null) throw new NullPointerException();
        return put2(key, value, true);
    }

    @Override
    public boolean remove(Object key, Object value) {
        if(key == null) throw new NullPointerException();
        if(value == null) return false;
        return remove2((Long)key, (Long)value)!=null;
    }

    @Override
    public boolean replace(final Long key, final Long oldValue, final Long newValue) {
        if(key == null || oldValue == null || newValue == null ) throw new NullPointerException();

        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode node = engine.get(current, nodeSerializer);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = engine.get(current, nodeSerializer);
        }

        lock(nodeLocks, current);
        try{

        LeafNode leaf = (LeafNode) engine.get(current, nodeSerializer);
        int pos = findChildren(key, leaf.keys);

        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            lock(nodeLocks, leaf.next);
            unlock(nodeLocks, current);
            current = leaf.next;
            leaf = (LeafNode) engine.get(current, nodeSerializer);
            pos = findChildren(key, leaf.keys);
        }

        boolean ret = false;
        if( key!=null && leaf.keys[pos]!=null &&
                Fun.COMPARATOR.compare(key,leaf.keys[pos])==0){
            Long val  = leaf.vals[pos-1];
            
            if(oldValue.equals(val)){
                Long[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
                notify(key, oldValue, newValue);
                vals[pos-1] = newValue;

                leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);

                assert(nodeLocks.get(current)== Thread.currentThread());
                engine.update(current, leaf, nodeSerializer);

                ret = true;
            }
        }
        unlock(nodeLocks, current);
        return ret;
        }catch(RuntimeException e){
            unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            unlockAll(nodeLocks);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long replace(final Long key, final Long value) {
        if(key == null || value == null) throw new NullPointerException();
        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode node = engine.get(current, nodeSerializer);
        //dive until leaf is found
        while(!node.isLeaf()){
            current = nextDir((DirNode) node, key);
            node = engine.get(current, nodeSerializer);
        }

        lock(nodeLocks, current);
        try{

        LeafNode leaf = (LeafNode) engine.get(current, nodeSerializer);
        int pos = findChildren(key, leaf.keys);

        while(pos==leaf.keys.length){
            //follow leaf link until necessary
            lock(nodeLocks, leaf.next);
            unlock(nodeLocks, current);
            current = leaf.next;
            leaf = (LeafNode) engine.get(current, nodeSerializer);
            pos = findChildren(key, leaf.keys);
        }

        Long ret = null;
        if( key!=null && leaf.keys()[pos]!=null &&
                0==Fun.COMPARATOR.compare(key,leaf.keys[pos])){
            Long[] vals = Arrays.copyOf(leaf.vals, leaf.vals.length);
            Long oldVal = vals[pos-1];
            ret =  (oldVal);
            notify(key, (Long)ret, value);
            vals[pos-1] = value;

            leaf = new LeafNode(Arrays.copyOf(leaf.keys, leaf.keys.length), vals, leaf.next);
            assert(nodeLocks.get(current)== Thread.currentThread());
            engine.update(current, leaf, nodeSerializer);


        }
        unlock(nodeLocks, current);
        return (Long)ret;
        }catch(RuntimeException e){
            unlockAll(nodeLocks);
            throw e;
        }catch(Exception e){
            unlockAll(nodeLocks);
            throw new RuntimeException(e);
        }

    }


    @Override
    public Comparator<? super Long> comparator() {
        return Fun.COMPARATOR;
    }


    @Override
    public Map.Entry<Long, Long> firstEntry() {
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        BNode n = engine.get(rootRecid, nodeSerializer);
        while(!n.isLeaf()){
            n = engine.get(n.child()[0], nodeSerializer);
        }
        LeafNode l = (LeafNode) n;
        //follow link until necessary
        while(l.keys.length==2){
            if(l.next==0) return null;
            l = (LeafNode) engine.get(l.next, nodeSerializer);
        }
        return makeEntry(l.keys[1], (l.vals[0]));
    }


    @Override
    public Entry<Long, Long> pollFirstEntry() {
        while(true){
            Entry<Long, Long> e = firstEntry();
            if(e==null || remove(e.getKey(),e.getValue())){
                return e;
            }
        }
    }

    @Override
    public Entry<Long, Long> pollLastEntry() {
        while(true){
            Entry<Long, Long> e = lastEntry();
            if(e==null || remove(e.getKey(),e.getValue())){
                return e;
            }
        }
    }


    protected Entry<Long, Long> findSmaller(Long key,boolean inclusive){
        if(key==null) throw new NullPointerException();
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        BNode n = engine.get(rootRecid, nodeSerializer);

        Entry<Long, Long> k = findSmallerRecur(n, key, inclusive);
        if(k==null || (k.getValue()==null)) return null;
        return k;
    }

    private Entry<Long, Long> findSmallerRecur(BNode n, Long key, boolean inclusive) {
        final boolean leaf = n.isLeaf();
        final int start = leaf ? n.keys().length-2 : n.keys().length-1;
        final int end = leaf?1:0;
        final int res = inclusive? 1 : 0;
        for(int i=start;i>=end; i--){
            final Object key2 = n.keys()[i];
            int comp = (key2==null)? -1 : Fun.COMPARATOR.compare(key2, key);
            if(comp<res){
                if(leaf){
                    return key2==null ? null :
                            makeEntry(key2, (n.vals()[i-1]));
                }else{
                    final long recid = n.child()[i];
                    if(recid==0) continue;
                    BNode n2 = engine.get(recid, nodeSerializer);
                    Entry<Long, Long> ret = findSmallerRecur(n2, key, inclusive);
                    if(ret!=null) return ret;
                }
            }
        }

        return null;
    }


    @Override
    public Map.Entry<Long, Long> lastEntry() {
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        BNode n = engine.get(rootRecid, nodeSerializer);
        Entry e = lastEntryRecur(n);
        if(e!=null && e.getValue()==null) return null;
        return e;
    }


    private Map.Entry<Long, Long> lastEntryRecur(BNode n){
        if(n.isLeaf()){
            //follow next node if available
            if(n.next()!=0){
                BNode n2 = engine.get(n.next(), nodeSerializer);
                Map.Entry<Long, Long> ret = lastEntryRecur(n2);
                if(ret!=null)
                    return ret;
            }

            //iterate over keys to find last non null key
            for(int i=n.keys().length-2; i>0;i--){
                Object k = n.keys()[i];
                if(k!=null && n.vals().length>0) {
                    Object val = (n.vals()[i-1]);
                    if(val!=null){
                        return makeEntry(k, val);
                    }
                }
            }
        }else{
            //dir node, dive deeper
            for(int i=n.child().length-1; i>=0;i--){
                long childRecid = n.child()[i];
                if(childRecid==0) continue;
                BNode n2 = engine.get(childRecid, nodeSerializer);
                Entry<Long, Long> ret = lastEntryRecur(n2);
                if(ret!=null)
                    return ret;
            }
        }
        return null;
    }

    @Override
	public Map.Entry<Long, Long> lowerEntry(Long key) {
        if(key==null) throw new NullPointerException();
        return findSmaller(key, false);
    }

    @Override
	public Long lowerKey(Long key) {
        Entry<Long, Long> n = lowerEntry(key);
        return (n == null)? null : n.getKey();
    }

    @Override
	public Map.Entry<Long, Long> floorEntry(Long key) {
        if(key==null) throw new NullPointerException();
        return findSmaller(key, true);
    }

    @Override
	public Long floorKey(Long key) {
        Entry<Long, Long> n = floorEntry(key);
        return (n == null)? null : n.getKey();
    }

    @Override
	public Map.Entry<Long, Long> ceilingEntry(Long key) {
        if(key==null) throw new NullPointerException();
        return findLarger(key, true);
    }

    protected Entry<Long, Long> findLarger(final Long key, boolean inclusive) {
        if(key==null) return null;

        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode A = engine.get(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, key);
            A = engine.get(current, nodeSerializer);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        //follow link until first matching node is found
        final int comp = inclusive?1:0;
        while(true){
            for(int i=1;i<leaf.keys.length-1;i++){
                if(leaf.keys[i]==null) continue;

                if(Fun.COMPARATOR.compare(key, leaf.keys[i])<comp){
                    return makeEntry(leaf.keys[i], (leaf.vals[i-1]));
                }


            }
            if(leaf.next==0) return null; //reached end
            leaf = (LeafNode) engine.get(leaf.next, nodeSerializer);
        }

    }

    protected Fun.Tuple2<Integer,LeafNode> findLargerNode(final Long key, boolean inclusive) {
        if(key==null) return null;

        long current = engine.get(rootRecidRef, Serializer.LONG);

        BNode A = engine.get(current, nodeSerializer);

        //dive until  leaf
        while(!A.isLeaf()){
            current = nextDir((DirNode) A, key);
            A = engine.get(current, nodeSerializer);
        }

        //now at leaf level
        LeafNode leaf = (LeafNode) A;
        //follow link until first matching node is found
        final int comp = inclusive?1:0;
        while(true){
            for(int i=1;i<leaf.keys.length-1;i++){
                if(leaf.keys[i]==null) continue;

                if(Fun.COMPARATOR.compare(key, leaf.keys[i])<comp){
                    return Fun.t2(i, leaf);
                }
            }
            if(leaf.next==0) return null; //reached end
            leaf = (LeafNode) engine.get(leaf.next, nodeSerializer);
        }

    }


    @Override
	public Long ceilingKey(Long key) {
        if(key==null) throw new NullPointerException();
        Entry<Long, Long> n = ceilingEntry(key);
        return (n == null)? null : n.getKey();
    }

    @Override
	public Map.Entry<Long, Long> higherEntry(Long key) {
        if(key==null) throw new NullPointerException();
        return findLarger(key, false);
    }

    @Override
	public Long higherKey(Long key) {
        if(key==null) throw new NullPointerException();
        Entry<Long, Long> n = higherEntry(key);
        return (n == null)? null : n.getKey();
    }

    @Override
    public boolean containsKey(Object key) {
        if(key==null) throw new NullPointerException();
        return get(key, false)!=null;
    }

    @Override
    public boolean containsValue(Object value){
        if(value ==null) throw new NullPointerException();
        Iterator<Long> valueIter = valueIterator();
        while(valueIter.hasNext()){
            if(value.equals(valueIter.next()))
                return true;
        }
        return false;
    }


    @Override
    public Long firstKey() {
        Entry<Long, Long> e = firstEntry();
        if(e==null) throw new NoSuchElementException();
        return e.getKey();
    }

    @Override
    public Long lastKey() {
        Entry<Long, Long> e = lastEntry();
        if(e==null) throw new NoSuchElementException();
        return e.getKey();
    }


    @Override
    public ConcurrentNavigableMap<Long, Long> subMap(Long fromKey,
                                              boolean fromInclusive,
                                              Long toKey,
                                              boolean toInclusive) {
        if (fromKey == null || toKey == null)
            throw new NullPointerException();
        return new SubMap
                ( this, fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public ConcurrentNavigableMap<Long, Long> headMap(Long toKey,
                                               boolean inclusive) {
        if (toKey == null)
            throw new NullPointerException();
        return new SubMap
                (this, null, false, toKey, inclusive);
    }

    @Override
    public ConcurrentNavigableMap<Long, Long> tailMap(Long fromKey,
                                               boolean inclusive) {
        if (fromKey == null)
            throw new NullPointerException();
        return new SubMap
                (this, fromKey, inclusive, null, false);
    }

    @Override
    public ConcurrentNavigableMap<Long, Long> subMap(Long fromKey, Long toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<Long, Long> headMap(Long toKey) {
        return headMap(toKey, false);
    }

    @Override
    public ConcurrentNavigableMap<Long, Long> tailMap(Long fromKey) {
        return tailMap(fromKey, true);
    }


    Iterator<Long> keyIterator() {
        return new BTreeKeyIterator(this);
    }

    Iterator<Long> valueIterator() {
        return new BTreeValueIterator(this);
    }

    Iterator<Entry<Long, Long>> entryIterator() {
        return new BTrLongntryIterator(this);
    }


    /* ---------------- View methods -------------- */

    @Override
	public NavigableSet<Long> keySet() {
        return keySet;
    }

    @Override
	public NavigableSet<Long> navigableKeySet() {
        return keySet;
    }

    @Override
	public Collection<Long> values() {
        return values;
    }

    @Override
	public Set<Entry<Long, Long>> entrySet() {
        return entrySet;
    }

    @Override
	public ConcurrentNavigableMap<Long, Long> descendingMap() {
        return descendingMap;
    }

    @Override
	public NavigableSet<Long> descendingKeySet() {
        return descendingMap.keySet();
    }

    static <E> List<E> toList(Collection<E> c) {
        // Using size() here would be a pessimization.
        List<E> list = new ArrayList<E>();
        for (E e : c){
            list.add(e);
        }
        return list;
    }



    static final class KeySet extends AbstractSet<Long> implements NavigableSet<Long> {

        protected final ConcurrentNavigableMap<Long,Long> m;

        KeySet(ConcurrentNavigableMap<Long,Long> map) {
            m = map;
        }
        @Override
		public int size() { return m.size(); }
        @Override
		public boolean isEmpty() { return m.isEmpty(); }
        @Override
		public boolean contains(Object o) { return m.containsKey(o); }
        @Override
		public boolean remove(Object o) { return m.remove(o) != null; }
        @Override
		public void clear() { m.clear(); }
        @Override
		public Long lower(Long e) { return m.lowerKey(e); }
        @Override
		public Long floor(Long e) { return m.floorKey(e); }
        @Override
		public Long ceiling(Long e) { return m.ceilingKey(e); }
        @Override
		public Long higher(Long e) { return m.higherKey(e); }
        @Override
		public Comparator<? super Long> comparator() { return Fun.COMPARATOR; }
        @Override
		public Long first() { return m.firstKey(); }
        @Override
		public Long last() { return m.lastKey(); }
        @Override
		public Long pollFirst() {
            Map.Entry<Long,Long> e = m.pollFirstEntry();
            return e == null? null : e.getKey();
        }
        @Override
		public Long pollLast() {
            Map.Entry<Long,Long> e = m.pollLastEntry();
            return e == null? null : e.getKey();
        }
        @Override
		public Iterator<Long> iterator() {
            if (m instanceof LongBTreeMap)
                return ((LongBTreeMap)m).keyIterator();
            else if(m instanceof SubMap)
                return ((LongBTreeMap.SubMap)m).keyIterator();
            else
                return ((LongBTreeMap.DescendingMap)m).keyIterator();
        }
        @Override
		public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof Set))
                return false;
            Collection<?> c = (Collection<?>) o;
            try {
                return containsAll(c) && c.containsAll(this);
            } catch (ClassCastException unused)   {
                return false;
            } catch (NullPointerException unused) {
                return false;
            }
        }
        @Override
		public Object[] toArray()     { return toList(this).toArray();  }
        @Override
		public <T> T[] toArray(T[] a) { return toList(this).toArray(a); }
        @Override
		public Iterator<Long> descendingIterator() {
            return descendingSet().iterator();
        }
        @Override
		public NavigableSet<Long> subSet(Long fromElement,
                                      boolean fromInclusive,
                                      Long toElement,
                                      boolean toInclusive) {
            return new KeySet(m.subMap(fromElement, fromInclusive,
                    toElement,   toInclusive));
        }
        @Override
		public NavigableSet<Long> headSet(Long toElement, boolean inclusive) {
            return new KeySet(m.headMap(toElement, inclusive));
        }
        @Override
		public NavigableSet<Long> tailSet(Long fromElement, boolean inclusive) {
            return new KeySet(m.tailMap(fromElement, inclusive));
        }
        @Override
		public NavigableSet<Long> subSet(Long fromElement, Long toElement) {
            return subSet(fromElement, true, toElement, false);
        }
        @Override
		public NavigableSet<Long> headSet(Long toElement) {
            return headSet(toElement, false);
        }
        @Override
		public NavigableSet<Long> tailSet(Long fromElement) {
            return tailSet(fromElement, true);
        }
        @Override
		public NavigableSet<Long> descendingSet() {
            return new KeySet(m.descendingMap());
        }

        @Override
        public boolean add(Long k) {
            throw new UnsupportedOperationException();
        }
    }

    static final class Values extends AbstractCollection<Long> {
        private final ConcurrentNavigableMap<Long,Long> m;
        Values(ConcurrentNavigableMap<Long, Long> map) {
            m = map;
        }
        @Override
		public Iterator<Long> iterator() {
            if (m instanceof LongBTreeMap)
                return ((LongBTreeMap)m).valueIterator();
            else
                return ((SubMap)m).valueIterator();
        }
        @Override
		public boolean isEmpty() {
            return m.isEmpty();
        }
        @Override
		public int size() {
            return m.size();
        }
        @Override
		public boolean contains(Object o) {
            return m.containsValue(o);
        }
        @Override
		public void clear() {
            m.clear();
        }
        @Override
		public Object[] toArray()     { return toList(this).toArray();  }
        @Override
		public <T> T[] toArray(T[] a) { return toList(this).toArray(a); }
    }

    static final class EntrySet extends AbstractSet<Entry<Long,Long>> {
        private final ConcurrentNavigableMap<Long,Long> m;
        EntrySet(ConcurrentNavigableMap<Long,Long> map) {
            m = map;
        }

        @Override
		public Iterator<Entry<Long,Long>> iterator() {
            if (m instanceof LongBTreeMap)
                return ((LongBTreeMap)m).entryIterator();
            else if(m instanceof  SubMap)
                return ((SubMap)m).entryIterator();
            else
                return ((DescendingMap)m).entryIterator();
        }

        @Override
		public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<Long,Long> e = (Map.Entry<Long,Long>)o;
            Long key = e.getKey();
            if(key == null) return false;
            Long v = m.get(key);
            return v != null && v.equals(e.getValue());
        }
        @Override
		public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<Long,Long> e = (Map.Entry<Long,Long>)o;
            Long key = e.getKey();
            if(key == null) return false;
            return m.remove(key,
                    e.getValue());
        }
        @Override
		public boolean isEmpty() {
            return m.isEmpty();
        }
        @Override
		public int size() {
            return m.size();
        }
        @Override
		public void clear() {
            m.clear();
        }
        @Override
		public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof Set))
                return false;
            Collection<?> c = (Collection<?>) o;
            try {
                return containsAll(c) && c.containsAll(this);
            } catch (ClassCastException unused)   {
                return false;
            } catch (NullPointerException unused) {
                return false;
            }
        }
        @Override
		public Object[] toArray()     { return toList(this).toArray();  }
        @Override
		public <T> T[] toArray(T[] a) { return toList(this).toArray(a); }
    }



    static protected  class SubMap extends AbstractMap<Long, Long> implements ConcurrentNavigableMap<Long, Long> {

        protected final LongBTreeMap m;

        protected final Long lo;
        protected final boolean loInclusive;

        protected final Long hi;
        protected final boolean hiInclusive;

        public SubMap(LongBTreeMap m, Long lo, boolean loInclusive, Long hi, boolean hiInclusive) {
            this.m = m;
            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            if(lo!=null && hi!=null && Fun.COMPARATOR.compare(lo, hi)>0){
                    throw new IllegalArgumentException();
            }


        }


/* ----------------  Map API methods -------------- */

        @Override
		public boolean containsKey(Object key) {
            if (key == null) throw new NullPointerException();
            Long k = (Long)key;
            return inBounds(k) && m.containsKey(k);
        }

        @Override
		public Long get(Object key) {
            if (key == null) throw new NullPointerException();
            Long k = (Long)key;
            return ((!inBounds(k)) ? null : m.get(k));
        }

        @Override
		public Long put(Long key, Long value) {
            checkKeyBounds(key);
            return m.put(key, value);
        }

        @Override
		public Long remove(Object key) {
            Long k = (Long)key;
            return (!inBounds(k))? null : m.remove(k);
        }

        @Override
        public int size() {
            Iterator<Long> i = keyIterator();
            int counter = 0;
            while(i.hasNext()){
                counter++;
                i.next();
            }
            return counter;
        }

        @Override
		public boolean isEmpty() {
            return !keyIterator().hasNext();
        }

        @Override
		public boolean containsValue(Object value) {
            if(value==null) throw new NullPointerException();
            Iterator<Long> i = valueIterator();
            while(i.hasNext()){
                if(value.equals(i.next()))
                    return true;
            }
            return false;
        }

        @Override
		public void clear() {
            Iterator<Long> i = keyIterator();
            while(i.hasNext()){
                i.next();
                i.remove();
            }
        }


        /* ----------------  ConcurrentMap API methods -------------- */

        @Override
		public Long putIfAbsent(Long key, Long value) {
            checkKeyBounds(key);
            return m.putIfAbsent(key, value);
        }

        @Override
		public boolean remove(Object key, Object value) {
            Long k = (Long)key;
            return inBounds(k) && m.remove(k, value);
        }

        @Override
		public boolean replace(Long key, Long oldValue, Long newValue) {
            checkKeyBounds(key);
            return m.replace(key, oldValue, newValue);
        }

        @Override
		public Long replace(Long key, Long value) {
            checkKeyBounds(key);
            return m.replace(key, value);
        }

        /* ----------------  SortedMap API methods -------------- */

        @Override
		public Comparator<? super Long> comparator() {
            return Fun.COMPARATOR;
        }

        /* ----------------  Relational methods -------------- */

        @Override
		public Map.Entry<Long, Long> lowerEntry(Long key) {
            if(key==null)throw new NullPointerException();
            if(tooLow(key))return null;

            if(tooHigh(key))
                return lastEntry();

            Entry<Long, Long> r = m.lowerEntry(key);
            return r!=null && !tooLow(r.getKey()) ? r :null;
        }

        @Override
		public Long lowerKey(Long key) {
            Entry<Long, Long> n = lowerEntry(key);
            return (n == null)? null : n.getKey();
        }

        @Override
		public Map.Entry<Long, Long> floorEntry(Long key) {
            if(key==null) throw new NullPointerException();
            if(tooLow(key)) return null;

            if(tooHigh(key)){
                return lastEntry();
            }

            Entry<Long, Long> ret = m.floorEntry(key);
            if(ret!=null && tooLow(ret.getKey())) return null;
            return ret;

        }

        @Override
		public Long floorKey(Long key) {
            Entry<Long, Long> n = floorEntry(key);
            return (n == null)? null : n.getKey();
        }

        @Override
		public Map.Entry<Long, Long> ceilingEntry(Long key) {
            if(key==null) throw new NullPointerException();
            if(tooHigh(key)) return null;

            if(tooLow(key)){
                return firstEntry();
            }

            Entry<Long, Long> ret = m.ceilingEntry(key);
            if(ret!=null && tooHigh(ret.getKey())) return null;
            return ret;
        }

        @Override
        public Long ceilingKey(Long key) {
            Entry<Long, Long> k = ceilingEntry(key);
            return k!=null? k.getKey():null;
        }

        @Override
        public Entry<Long, Long> higherEntry(Long key) {
            Entry<Long, Long> r = m.higherEntry(key);
            return r!=null && inBounds(r.getKey()) ? r : null;
        }

        @Override
        public Long higherKey(Long key) {
            Entry<Long, Long> k = higherEntry(key);
            return k!=null? k.getKey():null;
        }


        @Override
		public Long firstKey() {
            Entry<Long, Long> e = firstEntry();
            if(e==null) throw new NoSuchElementException();
            return e.getKey();
        }

        @Override
		public Long lastKey() {
            Entry<Long, Long> e = lastEntry();
            if(e==null) throw new NoSuchElementException();
            return e.getKey();
        }


        @Override
		public Map.Entry<Long, Long> firstEntry() {
            Entry<Long, Long> k =
                    lo==null ?
                    m.firstEntry():
                    m.findLarger(lo, loInclusive);
            return k!=null && inBounds(k.getKey())? k : null;

        }

        @Override
		public Map.Entry<Long, Long> lastEntry() {
            Entry<Long, Long> k =
                    hi==null ?
                    m.lastEntry():
                    m.findSmaller(hi, hiInclusive);

            return k!=null && inBounds(k.getKey())? k : null;
        }

        @Override
        public Entry<Long, Long> pollFirstEntry() {
            while(true){
                Entry<Long, Long> e = firstEntry();
                if(e==null || remove(e.getKey(),e.getValue())){
                    return e;
                }
            }
        }

        @Override
        public Entry<Long, Long> pollLastEntry() {
            while(true){
                Entry<Long, Long> e = lastEntry();
                if(e==null || remove(e.getKey(),e.getValue())){
                    return e;
                }
            }
        }




        /**
         * Utility to create submaps, where given bounds override
         * unbounded(null) ones and/or are checked against bounded ones.
         */
        private SubMap newSubMap(Long fromKey,
                                      boolean fromInclusive,
                                      Long toKey,
                                      boolean toInclusive) {

//            if(fromKey!=null && toKey!=null){
//                int comp = Fun.COMPARATOR.compare(fromKey, toKey);
//                if((fromInclusive||!toInclusive) && comp==0)
//                    throw new IllegalArgumentException();
//            }

            if (lo != null) {
                if (fromKey == null) {
                    fromKey = lo;
                    fromInclusive = loInclusive;
                }
                else {
                    int c = Fun.COMPARATOR.compare(fromKey, lo);
                    if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            if (hi != null) {
                if (toKey == null) {
                    toKey = hi;
                    toInclusive = hiInclusive;
                }
                else {
                    int c = Fun.COMPARATOR.compare(toKey, hi);
                    if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            return new SubMap(m, fromKey, fromInclusive,
                    toKey, toInclusive);
        }

        @Override
		public SubMap subMap(Long fromKey,
                                  boolean fromInclusive,
                                  Long toKey,
                                  boolean toInclusive) {
            if (fromKey == null || toKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        @Override
		public SubMap headMap(Long toKey,
                                   boolean inclusive) {
            if (toKey == null)
                throw new NullPointerException();
            return newSubMap(null, false, toKey, inclusive);
        }

        @Override
		public SubMap tailMap(Long fromKey,
                                   boolean inclusive) {
            if (fromKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, inclusive, null, false);
        }

        @Override
		public SubMap subMap(Long fromKey, Long toKey) {
            return subMap(fromKey, true, toKey, false);
        }

        @Override
		public SubMap headMap(Long toKey) {
            return headMap(toKey, false);
        }

        @Override
		public SubMap tailMap(Long fromKey) {
            return tailMap(fromKey, true);
        }

        @Override
		public ConcurrentNavigableMap<Long, Long> descendingMap() {
            return new DescendingMap(m, lo,loInclusive, hi,hiInclusive);
        }

        @Override
        public NavigableSet<Long> navigableKeySet() {
            return new KeySet((ConcurrentNavigableMap<Long,Long>) this);
        }


        /* ----------------  Utilities -------------- */



        private boolean tooLow(Long key) {
            if (lo != null) {
                int c = Fun.COMPARATOR.compare(key, lo);
                if (c < 0 || (c == 0 && !loInclusive))
                    return true;
            }
            return false;
        }

        private boolean tooHigh(Long key) {
            if (hi != null) {
                int c = Fun.COMPARATOR.compare(key, hi);
                if (c > 0 || (c == 0 && !hiInclusive))
                    return true;
            }
            return false;
        }

        private boolean inBounds(Long key) {
            return !tooLow(key) && !tooHigh(key);
        }

        private void checkKeyBounds(Long key) throws IllegalArgumentException {
            if (key == null)
                throw new NullPointerException();
            if (!inBounds(key))
                throw new IllegalArgumentException("key out of range");
        }





        @Override
        public NavigableSet<Long> keySet() {
            return new KeySet((ConcurrentNavigableMap<Long,Long>) this);
        }

        @Override
        public NavigableSet<Long> descendingKeySet() {
            return new DescendingMap(m,lo,loInclusive, hi, hiInclusive).keySet();
        }



        @Override
        public Set<Entry<Long, Long>> entrySet() {
            return new EntrySet(this);
        }



        Iterator<Long> keyIterator() {
            return new BTreeKeyIterator(m,lo,loInclusive,hi,hiInclusive);
        }

        Iterator<Long> valueIterator() {
            return new BTreeValueIterator(m,lo,loInclusive,hi,hiInclusive);
        }

        Iterator<Entry<Long, Long>> entryIterator() {
            return new BTrLongntryIterator(m,lo,loInclusive,hi,hiInclusive);
        }

    }


    static protected  class DescendingMap extends AbstractMap<Long, Long> implements ConcurrentNavigableMap<Long, Long> {

        protected final LongBTreeMap m;

        protected final Long lo;
        protected final boolean loInclusive;

        protected final Long hi;
        protected final boolean hiInclusive;

        public DescendingMap(LongBTreeMap map, Long lo, boolean loInclusive, Long hi, boolean hiInclusive) {
            this.m = map;
            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            if(lo!=null && hi!=null && Fun.COMPARATOR.compare(lo, hi)>0){
                throw new IllegalArgumentException();
            }


        }


/* ----------------  Map API methods -------------- */

        @Override
        public boolean containsKey(Object key) {
            if (key == null) throw new NullPointerException();
            Long k = (Long)key;
            return inBounds(k) && m.containsKey(k);
        }

        @Override
        public Long get(Object key) {
            if (key == null) throw new NullPointerException();
            Long k = (Long)key;
            return ((!inBounds(k)) ? null : m.get(k));
        }

        @Override
        public Long put(Long key, Long value) {
            checkKeyBounds(key);
            return m.put(key, value);
        }

        @Override
        public Long remove(Object key) {
            Long k = (Long)key;
            return (!inBounds(k))? null : m.remove(k);
        }

        @Override
        public int size() {
            Iterator<Long> i = keyIterator();
            int counter = 0;
            while(i.hasNext()){
                counter++;
                i.next();
            }
            return counter;
        }

        @Override
        public boolean isEmpty() {
            return !keyIterator().hasNext();
        }

        @Override
        public boolean containsValue(Object value) {
            if(value==null) throw new NullPointerException();
            Iterator<Long> i = valueIterator();
            while(i.hasNext()){
                if(value.equals(i.next()))
                    return true;
            }
            return false;
        }

        @Override
        public void clear() {
            Iterator<Long> i = keyIterator();
            while(i.hasNext()){
                i.next();
                i.remove();
            }
        }


        /* ----------------  ConcurrentMap API methods -------------- */

        @Override
        public Long putIfAbsent(Long key, Long value) {
            checkKeyBounds(key);
            return m.putIfAbsent(key, value);
        }

        @Override
        public boolean remove(Object key, Object value) {
            Long k = (Long)key;
            return inBounds(k) && m.remove(k, value);
        }

        @Override
        public boolean replace(Long key, Long oldValue, Long newValue) {
            checkKeyBounds(key);
            return m.replace(key, oldValue, newValue);
        }

        @Override
        public Long replace(Long key, Long value) {
            checkKeyBounds(key);
            return m.replace(key, value);
        }

        /* ----------------  SortedMap API methods -------------- */

        @Override
        public Comparator<? super Long> comparator() {
            return Fun.COMPARATOR;
        }

        /* ----------------  Relational methods -------------- */

        @Override
        public Map.Entry<Long, Long> higherEntry(Long key) {
            if(key==null)throw new NullPointerException();
            if(tooLow(key))return null;

            if(tooHigh(key))
                return firstEntry();

            Entry<Long, Long> r = m.lowerEntry(key);
            return r!=null && !tooLow(r.getKey()) ? r :null;
        }

        @Override
        public Long lowerKey(Long key) {
            Entry<Long, Long> n = lowerEntry(key);
            return (n == null)? null : n.getKey();
        }

        @Override
        public Map.Entry<Long, Long> ceilingEntry(Long key) {
            if(key==null) throw new NullPointerException();
            if(tooLow(key)) return null;

            if(tooHigh(key)){
                return firstEntry();
            }

            Entry<Long, Long> ret = m.floorEntry(key);
            if(ret!=null && tooLow(ret.getKey())) return null;
            return ret;

        }

        @Override
        public Long floorKey(Long key) {
            Entry<Long, Long> n = floorEntry(key);
            return (n == null)? null : n.getKey();
        }

        @Override
        public Map.Entry<Long, Long> floorEntry(Long key) {
            if(key==null) throw new NullPointerException();
            if(tooHigh(key)) return null;

            if(tooLow(key)){
                return lastEntry();
            }

            Entry<Long, Long> ret = m.ceilingEntry(key);
            if(ret!=null && tooHigh(ret.getKey())) return null;
            return ret;
        }

        @Override
        public Long ceilingKey(Long key) {
            Entry<Long, Long> k = ceilingEntry(key);
            return k!=null? k.getKey():null;
        }

        @Override
        public Entry<Long, Long> lowerEntry(Long key) {
            Entry<Long, Long> r = m.higherEntry(key);
            return r!=null && inBounds(r.getKey()) ? r : null;
        }

        @Override
        public Long higherKey(Long key) {
            Entry<Long, Long> k = higherEntry(key);
            return k!=null? k.getKey():null;
        }


        @Override
        public Long firstKey() {
            Entry<Long, Long> e = firstEntry();
            if(e==null) throw new NoSuchElementException();
            return e.getKey();
        }

        @Override
        public Long lastKey() {
            Entry<Long, Long> e = lastEntry();
            if(e==null) throw new NoSuchElementException();
            return e.getKey();
        }


        @Override
        public Map.Entry<Long, Long> lastEntry() {
            Entry<Long, Long> k =
                    lo==null ?
                            m.firstEntry():
                            m.findLarger(lo, loInclusive);
            return k!=null && inBounds(k.getKey())? k : null;

        }

        @Override
        public Map.Entry<Long, Long> firstEntry() {
            Entry<Long, Long> k =
                    hi==null ?
                            m.lastEntry():
                            m.findSmaller(hi, hiInclusive);

            return k!=null && inBounds(k.getKey())? k : null;
        }

        @Override
        public Entry<Long, Long> pollFirstEntry() {
            while(true){
                Entry<Long, Long> e = firstEntry();
                if(e==null || remove(e.getKey(),e.getValue())){
                    return e;
                }
            }
        }

        @Override
        public Entry<Long, Long> pollLastEntry() {
            while(true){
                Entry<Long, Long> e = lastEntry();
                if(e==null || remove(e.getKey(),e.getValue())){
                    return e;
                }
            }
        }




        /**
         * Utility to create submaps, where given bounds override
         * unbounded(null) ones and/or are checked against bounded ones.
         */
        private DescendingMap newSubMap(
                                      Long toKey,
                                      boolean toInclusive,
                                      Long fromKey,
                                      boolean fromInclusive) {

//            if(fromKey!=null && toKey!=null){
//                int comp = Fun.COMPARATOR.compare(fromKey, toKey);
//                if((fromInclusive||!toInclusive) && comp==0)
//                    throw new IllegalArgumentException();
//            }

            if (lo != null) {
                if (fromKey == null) {
                    fromKey = lo;
                    fromInclusive = loInclusive;
                }
                else {
                    int c = Fun.COMPARATOR.compare(fromKey, lo);
                    if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            if (hi != null) {
                if (toKey == null) {
                    toKey = hi;
                    toInclusive = hiInclusive;
                }
                else {
                    int c = Fun.COMPARATOR.compare(toKey, hi);
                    if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            return new DescendingMap(m, fromKey, fromInclusive,
                    toKey, toInclusive);
        }

        @Override
        public DescendingMap subMap(Long fromKey,
                                  boolean fromInclusive,
                                  Long toKey,
                                  boolean toInclusive) {
            if (fromKey == null || toKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
        }

        @Override
        public DescendingMap headMap(Long toKey,
                                   boolean inclusive) {
            if (toKey == null)
                throw new NullPointerException();
            return newSubMap(null, false, toKey, inclusive);
        }

        @Override
        public DescendingMap tailMap(Long fromKey,
                                   boolean inclusive) {
            if (fromKey == null)
                throw new NullPointerException();
            return newSubMap(fromKey, inclusive, null, false);
        }

        @Override
        public DescendingMap subMap(Long fromKey, Long toKey) {
            return subMap(fromKey, true, toKey, false);
        }

        @Override
        public DescendingMap headMap(Long toKey) {
            return headMap(toKey, false);
        }

        @Override
        public DescendingMap tailMap(Long fromKey) {
            return tailMap(fromKey, true);
        }

        @Override
        public ConcurrentNavigableMap<Long, Long> descendingMap() {
            if(lo==null && hi==null) return m;
            return m.subMap(lo,loInclusive,hi,hiInclusive);
        }

        @Override
        public NavigableSet<Long> navigableKeySet() {
            return new KeySet((ConcurrentNavigableMap<Long,Long>) this);
        }


        /* ----------------  Utilities -------------- */



        private boolean tooLow(Long key) {
            if (lo != null) {
                int c = Fun.COMPARATOR.compare(key, lo);
                if (c < 0 || (c == 0 && !loInclusive))
                    return true;
            }
            return false;
        }

        private boolean tooHigh(Long key) {
            if (hi != null) {
                int c = Fun.COMPARATOR.compare(key, hi);
                if (c > 0 || (c == 0 && !hiInclusive))
                    return true;
            }
            return false;
        }

        private boolean inBounds(Long key) {
            return !tooLow(key) && !tooHigh(key);
        }

        private void checkKeyBounds(Long key) throws IllegalArgumentException {
            if (key == null)
                throw new NullPointerException();
            if (!inBounds(key))
                throw new IllegalArgumentException("key out of range");
        }





        @Override
        public NavigableSet<Long> keySet() {
            return new KeySet((ConcurrentNavigableMap<Long,Long>) this);
        }

        @Override
        public NavigableSet<Long> descendingKeySet() {
            return new KeySet((ConcurrentNavigableMap<Long,Long>) descendingMap());
        }



        @Override
        public Set<Entry<Long, Long>> entrySet() {
            return new EntrySet(this);
        }


        /*
         * ITERATORS
         */

        abstract class Iter<E> implements Iterator<E> {
            Entry<Long, Long> current = DescendingMap.this.firstEntry();
            Entry<Long, Long> last = null;


            @Override
            public boolean hasNext() {
                return current!=null;
            }


            public void advance() {
                if(current==null) throw new NoSuchElementException();
                last = current;
                current = DescendingMap.this.higherEntry(current.getKey());
            }

            @Override
            public void remove() {
                if(last==null) throw new IllegalStateException();
                DescendingMap.this.remove(last.getKey());
                last = null;
            }

        }
        Iterator<Long> keyIterator() {
            return new Iter<Long>() {
                @Override
                public Long next() {
                    advance();
                    return last.getKey();
                }
            };
        }

        Iterator<Long> valueIterator() {
            return new Iter<Long>() {

                @Override
                public Long next() {
                    advance();
                    return last.getValue();
                }
            };
        }

        Iterator<Entry<Long, Long>> entryIterator() {
            return new Iter<Entry<Long, Long>>() {
                @Override
                public Entry<Long, Long> next() {
                    advance();
                    return last;
                }
            };
        }


    }


    /**
     * Make readonly snapshot view of current Map. Snapshot is immutable and not affected by modifications made by other threads.
     * Useful if you need consistent view on Map.
     *
     * Maintaining snapshot have some overhead, underlying Engine is closed after Map view is GCed.
     * Please make sure to release reference to this Map view, so snapshot view can be garbage collected.
     *
     * @return snapshot
     */
    public NavigableMap<Long, Long> snapshot(){
        Engine snapshot = TxEngine.createSnapshotFor(engine);

        return new LongBTreeMap(snapshot, rootRecidRef, maxNodeSize,
                counter==null?0L:counter.getRecid(), numberOfNodeMetas, false);
    }



    protected final Object modListenersLock = new Object();
    protected Bind.MapListener<Long, Long>[] modListeners = new Bind.MapListener[0];

    @Override
    public void modificationListenerAdd(Bind.MapListener<Long, Long> listener) {
        synchronized (modListenersLock){
            Bind.MapListener<Long, Long>[] modListeners2 =
                    Arrays.copyOf(modListeners, modListeners.length + 1);
            modListeners2[modListeners2.length-1] = listener;
            modListeners = modListeners2;
        }

    }

    @Override
    public void modificationListenerRemove(Bind.MapListener<Long, Long> listener) {
        synchronized (modListenersLock){
            for(int i=0;i<modListeners.length;i++){
                if(modListeners[i]==listener) modListeners[i]=null;
            }
        }
    }

    protected void notify(Long key, Long oldValue, Long newValue) {

        Bind.MapListener<Long, Long>[] modListeners2  = modListeners;
        for(Bind.MapListener<Long, Long> listener:modListeners2){
            if(listener!=null)
                listener.update(key, oldValue, newValue);
        }
    }


    /**
     * Closes underlying storage and releases all resources.
     * Used mostly with temporary collections where engine is not accessible.
     */
    public void close(){
        engine.close();
    }

    public Engine getEngine(){
        return engine;
    }


    public void printTreeStructure() {
        final long rootRecid = engine.get(rootRecidRef, Serializer.LONG);
        printRecur(this, rootRecid, "");
    }

    private static void printRecur(LongBTreeMap m, long recid, String s) {
        LongBTreeMap.BNode n = (LongBTreeMap.BNode) m.engine.get(recid, m.nodeSerializer);
        System.out.println(s+recid+"-"+n);
        if(!n.isLeaf()){
            for(int i=0;i<n.child().length-1;i++){
                long recid2 = n.child()[i];
                if(recid2!=0)
                    printRecur(m, recid2, s+"  ");
            }
        }
    }


    protected  static long[] arrayLongPut(final long[] array, final int pos, final long value) {
        final long[] ret = Arrays.copyOf(array, array.length + 1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos + 1, array.length - pos);
        }
        ret[pos] = value;
        return ret;
    }

    /** expand array size by 1, and put value at given position. No items from original array are lost*/
    protected static Long[] arrayPut(final Long[] array, final int pos, final Long value){
        final Long[] ret = Arrays.copyOf(array, array.length + 1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos + 1, array.length - pos);
        }
        ret[pos] = value;
        return ret;
    }

    protected static void assertNoLocks(LongConcurrentHashMap<Thread> locks){
        LongMap.LongMapIterator<Thread> i = locks.longMapIterator();
        Thread t =null;
        while(i.moveToNext()){
            if(t==null)
                t = Thread.currentThread();
            if(i.value()==t){
                throw new AssertionError("Node "+i.key()+" is still locked");
            }
        }
    }


    protected static void unlock(LongConcurrentHashMap<Thread> locks,final long recid) {
        final Thread t = locks.remove(recid);
        assert(t== Thread.currentThread()):("unlocked wrong thread");
    }

    protected static void unlockAll(LongConcurrentHashMap<Thread> locks) {
        final Thread t = Thread.currentThread();
        LongMap.LongMapIterator<Thread> iter = locks.longMapIterator();
        while(iter.moveToNext())
            if(iter.value()==t)
                iter.remove();
    }


    protected static void lock(LongConcurrentHashMap<Thread> locks, long recid){
        //feel free to rewrite, if you know better (more efficient) way

        final Thread currentThread = Thread.currentThread();
        //check node is not already locked by this thread
        assert(locks.get(recid)!= currentThread):("node already locked by current thread: "+recid);

        while(locks.putIfAbsent(recid, currentThread) != null){
            LockSupport.parkNanos(10);
        }
    }

    /**
     * Builds a byte array from the array of booleans, compressing up to 8 booleans per byte.
     *
     * Author of this method is Chris Alexander.
     *
     * @param bool The booleans to be compressed.
     * @return The fully compressed byte array.
     */
    protected static byte[] booleanToByteArray(boolean[] bool) {
        int boolLen = bool.length;
        int mod8 = boolLen%8;
        byte[] boolBytes = new byte[(boolLen/8)+((boolLen%8 == 0)?0:1)];

        boolean isFlushWith8 = mod8 == 0;
        int length = (isFlushWith8)?boolBytes.length:boolBytes.length-1;
        int x = 0;
        int boolByteIndex;
        for (boolByteIndex=0; boolByteIndex<length;) {
            byte b = (byte)	(((bool[x++]? 0x01 : 0x00) << 0) |
                    ((bool[x++]? 0x01 : 0x00) << 1) |
                    ((bool[x++]? 0x01 : 0x00) << 2) |
                    ((bool[x++]? 0x01 : 0x00) << 3) |
                    ((bool[x++]? 0x01 : 0x00) << 4) |
                    ((bool[x++]? 0x01 : 0x00) << 5) |
                    ((bool[x++]? 0x01 : 0x00) << 6) |
                    ((bool[x++]? 0x01 : 0x00) << 7));
            boolBytes[boolByteIndex++] = b;
        }
        if (!isFlushWith8) {//If length is not a multiple of 8 we must do the last byte conditionally on every element.
            byte b = (byte)	0x00;

            switch(mod8) {
                case 1:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0);
                    break;
                case 2:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1);
                    break;
                case 3:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2);
                    break;
                case 4:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2) |
                            ((bool[x++]? 0x01 : 0x00) << 3);
                    break;
                case 5:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2) |
                            ((bool[x++]? 0x01 : 0x00) << 3) |
                            ((bool[x++]? 0x01 : 0x00) << 4);
                    break;
                case 6:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2) |
                            ((bool[x++]? 0x01 : 0x00) << 3) |
                            ((bool[x++]? 0x01 : 0x00) << 4) |
                            ((bool[x++]? 0x01 : 0x00) << 5);
                    break;
                case 7:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2) |
                            ((bool[x++]? 0x01 : 0x00) << 3) |
                            ((bool[x++]? 0x01 : 0x00) << 4) |
                            ((bool[x++]? 0x01 : 0x00) << 5) |
                            ((bool[x++]? 0x01 : 0x00) << 6);
                    break;
                case 8:
                    b |=	((bool[x++]? 0x01 : 0x00) << 0) |
                            ((bool[x++]? 0x01 : 0x00) << 1) |
                            ((bool[x++]? 0x01 : 0x00) << 2) |
                            ((bool[x++]? 0x01 : 0x00) << 3) |
                            ((bool[x++]? 0x01 : 0x00) << 4) |
                            ((bool[x++]? 0x01 : 0x00) << 5) |
                            ((bool[x++]? 0x01 : 0x00) << 6) |
                            ((bool[x++]? 0x01 : 0x00) << 7);
                    break;
            }

            ////////////////////////
            // OLD
            ////////////////////////
			/* The code below was replaced with the switch statement
			 * above. It increases performance by only doing 1
			 * check against mod8 (switch statement) and only doing 1
			 * assignment operation for every possible value of mod8,
			 * rather than doing up to 8 assignment operations and an
			 * if check in between each one. The code is longer but
			 * faster.
			 *
			byte b = (byte)	0x00;
			b |= ((bool[x++]? 0x01 : 0x00) << 0);
			if (mod8 > 1) {
				b |= ((bool[x++]? 0x01 : 0x00) << 1);
				if (mod8 > 2) {
					b |= ((bool[x++]? 0x01 : 0x00) << 2);
					if (mod8 > 3) {
						b |= ((bool[x++]? 0x01 : 0x00) << 3);
						if (mod8 > 4) {
							b |= ((bool[x++]? 0x01 : 0x00) << 4);
							if (mod8 > 5) {
								b |= ((bool[x++]? 0x01 : 0x00) << 5);
								if (mod8 > 6) {
									b |= ((bool[x++]? 0x01 : 0x00) << 6);
									if (mod8 > 7) {
										b |= ((bool[x++]? 0x01 : 0x00) << 7);
									}
								}
							}
						}
					}
				}
			}
			*/
            boolBytes[boolByteIndex++] = b;
        }

        return boolBytes;
    }



    /**
     * Unpacks an integer from the DataInput indicating the number of booleans that are compressed. It then calculates
     * the number of bytes, reads them in, and decompresses and converts them into an array of booleans using the
     * toBooleanArray(byte[]); method. The array of booleans are trimmed to <code>numBools</code> elements. This is
     * necessary in situations where the number of booleans is not a multiple of 8.
     *
     * Author of this method is Chris Alexander.
     *
     * @return The boolean array decompressed from the bytes read in.
     * @throws IOException If an error occurred while reading.
     */
    protected static boolean[] readBooleanArray(int numBools,DataInput is) throws IOException {
        int length = (numBools/8)+((numBools%8 == 0)?0:1);
        byte[] boolBytes = new byte[length];
        is.readFully(boolBytes);


        boolean[] tmp = new boolean[boolBytes.length*8];
        int len = boolBytes.length;
        int boolIndex = 0;
        for (byte boolByte : boolBytes) {
            for (int y = 0; y < 8; y++) {
                tmp[boolIndex++] = (boolByte & (0x01 << y)) != 0x00;
            }
        }

        //Trim excess booleans
        boolean[] finalBoolArray = new boolean[numBools];
        System.arraycopy(tmp, 0, finalBoolArray, 0, numBools);

        //Return the trimmed, uncompressed boolean array
        return finalBoolArray;
    }


}
