package exercise4.indexing.secondary.impl;


import exercise4.indexing.secondary.SecondaryIndex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * An implementation of a SecondaryIndex around a HashMap.
 */


 public class HashIndex implements SecondaryIndex {
    // TODO: add field(s) as necessary
    private final HashMap<Object, List<Long>> secondaryHashIndex;

    public HashIndex() throws Exception {
        // TODO - Done
        try {
            this.secondaryHashIndex = new HashMap<>();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new Exception("Error creating HashIndex");
        }
        
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void insert(Object value, Long tid){
        // TODO -done
        try {
            if(value == null || tid == null){
                System.out.println("Neither value nor tid can be null");
            }
            else{
                if(!secondaryHashIndex.containsKey(value)){
                    secondaryHashIndex.put(value, new ArrayList<>());
                }
                
                secondaryHashIndex.get(value).add(tid);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error inserting into HashIndex");
        }
        
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Long> get(Object value) {
        // TODO-done
        try {
            List<Long> tids = secondaryHashIndex.get(value);
            return (tids != null) ? tids : new ArrayList<>();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error getting from HashIndex");
            return new ArrayList<>();
        }
        
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean remove(Long tid, Object value) {

        try{
            List<Long> tids = secondaryHashIndex.get(value);
            if (tids != null) {
                boolean removed = tids.remove(tid);
                if (tids.isEmpty()) {
                    secondaryHashIndex.remove(value);
                }
                return removed;
            }
            else{
                System.out.println("No TIDs found for the given value");
            }
            return false;
        }
        catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Error removing from HashIndex");
            return false;
        }
        
        // TODO: make sure to only delete the given row/tid
        //throw new UnsupportedOperationException("Not supported yet.");
    }
}