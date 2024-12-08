package exercise4.indexing.utils;


import exercise4.indexing.primary.PrimaryIndex;
import exercise4.indexing.secondary.SecondaryIndex;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Table {
    private final Schema schema;
    private final PrimaryIndex primaryIndex;
    private final SecondaryIndex[] secondaryIndexes;

    public Table(Schema schema, PrimaryIndex primaryIndex) {
        this.schema = schema;
        this.primaryIndex = primaryIndex;
        this.secondaryIndexes = new SecondaryIndex[schema.columnCount()];
    }

    /**
     * Set the secondary index for the respective column, replacing any existing index.
     * Rows already in the table must be added to the index.
     */
    public void setSecondaryIndex(int columnIndex, SecondaryIndex index) {
        this.primaryIndex.scan().forEach(entry -> index
                .insert(entry.getValue().getColumn(columnIndex), entry.getKey()));
        this.secondaryIndexes[columnIndex] = index;
    }

    /**
     * Insert a row into the table by assigning a new TID and adding it to all primary/secondary indexes.
     */
    public void insert(Row row) {
        // TODO-done
        try {
            long ids = primaryIndex.insert(row);

            for (int i = 0; i < schema.columnCount(); i++) {
                if (secondaryIndexes[i] != null) {
                    secondaryIndexes[i].insert(row.getColumn(i), ids);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error inserting into Table");
        }
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean remove(ResultSet resultSet) {
        // TODO -done
        try {
            boolean allRemoved = true;

            for (Long tid :resultSet.getTids()) {
                Row r = primaryIndex.remove(tid);

                if (r == null) {
                    allRemoved = false;
                    continue;
                }

                for (int i = 0; i < schema.columnCount(); i++) {
                    if (secondaryIndexes[i] != null) {
                        secondaryIndexes[i].remove(tid, r.getColumn(i));
                    }
                }
            }
            return allRemoved;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error removing from Table");
            return false;
        }
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Perform a point query on the table. Returns the ResultSet of all rows with the respective column value at the given column.
     */
    public ResultSet pointQueryAtColumn(int columnIndex, Object value) {
        // TODO -onit


        try {
            Set<Long> ids = new HashSet<>();

            if (secondaryIndexes[columnIndex] != null) {
                ids.addAll(secondaryIndexes[columnIndex].get(value));
            } else {
                primaryIndex.scan().forEach(entry -> {
                    if (entry.getValue().getColumn(columnIndex).equals(value)) {
                        ids.add(entry.getKey());
                    }
                });
            }

            return new ResultSet(primaryIndex, ids);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error performing point query at column");
            return new ResultSet(primaryIndex, new HashSet<>());
        }

        //throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Perform a range query on the table. Returns the ResultSet of all rows with column value at columnIndex in the interval [from, to).
     */
    public ResultSet rangeQueryAtColumn(int columnIndex, Object from, Object to) {
        // TODO - does not work, revisit later

        try {
            if (schema.getComparatorOfColumn(columnIndex) == null) {
            throw new Exception("Invalid column index");
        }

        Set<Long> ids = new HashSet<>();

        if (secondaryIndexes[columnIndex] != null) {
            List<Long> rangeTids = secondaryIndexes[columnIndex].getRange(from, to);
            if (rangeTids != null) {
                ids.addAll(rangeTids);
            }
        } else {
            primaryIndex.scan().forEach(entry -> {
                Object v = entry.getValue().getColumn(columnIndex);
                Comparator<Object> comparator = schema.getComparatorOfColumn(columnIndex);

                boolean conditionOne = comparator.compare(v, from) >= 0;
                boolean conditionTwo = comparator.compare(v, to) < 0;

                if (conditionOne && conditionTwo) {
                    ids.add(entry.getKey());
                }
            });
        }

        return new ResultSet(primaryIndex, ids);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error performing range query at column");
            return new ResultSet(primaryIndex, new HashSet<>());
        }

        //throw new UnsupportedOperationException("Not supported yet.");
    }

    // Getters - TO GET VALUE IN MAIN.JAVA
    public PrimaryIndex getPrimaryIndex() {
        return primaryIndex;
    }
    
}