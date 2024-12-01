package exercise4.indexing.secondary.impl;


import exercise4.indexing.secondary.SecondaryIndex;
import java.util.*;

/**
 * An implementation of a SecondaryIndex around a TreeMap.
 */
public class SecondaryTreeIndex implements SecondaryIndex {
    // TODO: add field(s) as necessary
    private final TreeMap<Object, List<Long>> secondaryIndex;

    public SecondaryTreeIndex(Comparator<Object> cmp) {
        // TODO-done
        //throw new UnsupportedOperationException("Not supported yet.");
        this.secondaryIndex = new TreeMap<>(cmp);
    }

    @Override
    public void insert(Object value, Long tid) {
        // TODO-done
        try {
            if(value == null || tid == null){
                System.out.println("Neither value nor tid can be null");
            }
            else{
                if(!secondaryIndex.containsKey(value)){
                    secondaryIndex.put(value, new ArrayList<>());
                }

                secondaryIndex.get(value).add(tid);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error inserting into SeconfaryTreeIndex");
        }
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Long> get(Object value) {
        // TODO-done

        try {
            List<Long> ids = secondaryIndex.get(value);
            return (ids != null) ? ids : new ArrayList<>();
        }
        
        catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error reading from Secondary Tree Index");
            return new ArrayList<>();
        }

        //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean remove(Long tid, Object value) {
        // TODO: make sure to only delete the given row/tid

        try {
            List<Long> ids = secondaryIndex.get(value);
            if (ids != null) {
                boolean removed = ids.remove(tid);
                if (ids.isEmpty()) {
                    secondaryIndex.remove(value);
                }
                return removed;
            } else {
                System.out.println("No TID found for the given value");
            }
            return false;

        }
        catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error removing from TreeIndex");
            return false;
        }
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Long> getRange(Object from, Object to) {
        // TODO-on it
        try {
            NavigableMap<Object, List<Long>> subMap = secondaryIndex.subMap(from, true, to, false);
            List<Long> ids = new ArrayList<>();

            for (List<Long> tidList : subMap.values()) {
                ids.addAll(tidList);
            }
            return ids;
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error performing range query on TreeIndex");
            return new ArrayList<>();
        }
        //throw new UnsupportedOperationException("Not supported yet.");
    }
}
