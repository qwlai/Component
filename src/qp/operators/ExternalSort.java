package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

import qp.utils.AppendingObjectOutputStream;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

public class ExternalSort extends Operator {

    private static int instanceNum = 0;
    private final String FILE_HEADER = "EStemp-";

    private Operator table;
    private int numBuff;

    private int batchSize; // maximum number of tuples per batch

    private int fileNum;
    private int passNum;
    private int runNum;

    private Attribute joinAttribute;
    private Vector joinAttributes = null; // For distinct

    private Vector<File> sortedRunFiles;

    private ObjectInputStream in;

    private Comparator<Tuple> comparator;

    private boolean isDistinct = false;

    public ExternalSort(Operator table, Attribute joinAttribute, int numBuff, boolean flag) {
        super(OpType.SORT);
        this.table = table;
        this.numBuff = numBuff;
        this.batchSize = Batch.getPageSize()/table.getSchema().getTupleSize();
        this.joinAttribute = joinAttribute;
        this.fileNum = instanceNum++;
        this.isDistinct = flag;
    }

    // Overloaded constructor for removing duplicates
    public ExternalSort(Operator table, Vector joinAttributes, int numBuff, boolean flag) {
        super(OpType.SORT);
        this.table = table;
        this.numBuff = numBuff;
        this.batchSize = Batch.getPageSize()/table.getSchema().getTupleSize();
        this.joinAttributes = joinAttributes;
        this.fileNum = instanceNum++;
        this.isDistinct = flag;
    }

    public boolean open() {
        if (!table.open()) {
            return false;
        }

        passNum = 0;
        runNum = 0;
        sortedRunFiles = new Vector<>();
        comparator = isDistinct ? generateComparator(joinAttributes, isDistinct) : generateComparator();

        /** Phase 1 */
        generateRuns();
        passNum++;
        runNum = 0;

        /** Phase 2 */
        executeMerge();

        return true;
    }

    public Batch next() {
        try {
            if (in == null) {
                in = new ObjectInputStream(new FileInputStream(sortedRunFiles.get(0)));
            }
            return readBatch(in);
        } catch (IOException ioe) {
            System.out.println("ExternalSort: next() error");
        } catch (ArrayIndexOutOfBoundsException aioofe) { // no join result
            return null;
        }
        return null;
    }

    public boolean close() {
        try {
            in.close();
        } catch (IOException io) {
            System.out.println("ExternalSort: close() error");
        } catch (NullPointerException npe) {
            System.out.println("ExternalSort: no join result");
        }
        clearTempFiles(sortedRunFiles);
        return super.close();
    }

    // Phase 1
    private void generateRuns() {
        Vector<Batch> batchList = new Vector<>();
        Batch currBatch = table.next(); // first batch

        while (currBatch != null) {
            for (int i = 0; i < numBuff; i++) {
                batchList.add(currBatch);
                currBatch = table.next();

                if (currBatch == null)
                    break;
            }

            Vector<Batch> sortedRun = generateSortedRun(batchList);
            File sortedRunFile = writeRun(sortedRun);
            sortedRunFiles.add(sortedRunFile);
        }
        table.close();
    }

    private Vector<Batch> generateSortedRun(Vector<Batch> batchList) {
        Vector<Tuple> tupleList = new Vector<>();
        for (int i = 0; i < batchList.size(); i++) {
            Batch batch = batchList.get(i);
            for (int j = 0; j < batch.size(); j++) {
                tupleList.add(batch.elementAt(j));
            }
        }
        tupleList.sort(comparator);
        Vector<Batch> sortedRun = new Vector<>();
        Batch currentBatch = new Batch(batchSize);
        for (Tuple tuple : tupleList) {
            currentBatch.add(tuple);
            if (currentBatch.isFull()) {
                sortedRun.add(currentBatch);
                currentBatch = new Batch(batchSize);
            }
        }
        if (!currentBatch.isFull()) {
            sortedRun.add(currentBatch);
        }

        return sortedRun;
    }

    // Phase 2
    private void executeMerge() {
        int numUsableBuff = numBuff - 1;

        // Actually > 0 can... Leave it first
        while (sortedRunFiles.size() > 1) {
            int totalSortedRuns = sortedRunFiles.size();
            Vector<File> newSortedRuns = new Vector<>();
            for (int i = 0; i * numUsableBuff < totalSortedRuns; i++) {
                int startIndex = i * numUsableBuff;
                int endIndex = (i + 1) * numUsableBuff;
                endIndex = Math.min(endIndex, sortedRunFiles.size()); // for last runs

                List<File> runsToSort = sortedRunFiles.subList(startIndex, endIndex);
                File newSortedRun = mergeSortedRuns(runsToSort);
                newSortedRuns.add(newSortedRun);
            }

            passNum++;
            runNum = 0;

            clearTempFiles(sortedRunFiles);
            sortedRunFiles = newSortedRuns;
        } if (sortedRunFiles.size() == 1) {
            int totalSortedRuns = 1;
            Vector<File> newSortedRuns = new Vector<>();
            for (int i = 0; i * numUsableBuff < totalSortedRuns; i++) {
                int startIndex = i * numUsableBuff;
                int endIndex = (i + 1) * numUsableBuff;
                endIndex = Math.min(endIndex, sortedRunFiles.size()); // for last runs

                List<File> runsToSort = sortedRunFiles.subList(startIndex, endIndex);
                File newSortedRun = mergeSortedRuns(runsToSort);
                newSortedRuns.add(newSortedRun);
            }

            passNum++;
            runNum = 0;

            clearTempFiles(sortedRunFiles);
            sortedRunFiles = newSortedRuns;
        }
    }

    /**
     * Reads in list of files that store the sorted runs, merge them, and produce longer runs
     */
    private File mergeSortedRuns(List<File> sortedRuns) {
        if (sortedRuns.isEmpty())
            return null;

        int numBuffers = sortedRuns.size();
        Vector<Batch> inputBuffers = new Vector<>();

        List<ObjectInputStream> inputStreams = new Vector<>();

        // Reading into input buffers
        for (File file : sortedRuns) {
            try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
                inputStreams.add(ois);
            } catch (IOException ioe) {
                System.out.println("ExternalSort: Error in reading sorted runs at mergeSortedRuns");
            }
        }

        for (ObjectInputStream ois : inputStreams) {
            Batch batch = readBatch(ois);
            inputBuffers.add(batch);
        }

        // Merging
        Batch outputBuffer = new Batch(batchSize);
        File output = null;
        int[] batchTrackers = new int[numBuffers];
        Tuple lastTuple = null;

        while (true) {
            Tuple smallest = null;
            int indexOfSmallest = 0;
            for (int i = 0; i < inputBuffers.size(); i++) {
                Batch batch = inputBuffers.get(i);
                if (batchTrackers[i] >= batch.size())
                    continue;

                Tuple tuple = batch.elementAt(batchTrackers[i]);
                if (smallest == null || comparator.compare(tuple, smallest) < 0) {
                    smallest = tuple;
                    indexOfSmallest = i;
                }
            }

            if (smallest == null)
                break;

            batchTrackers[indexOfSmallest]++; // increase batch index containing smallest
            // if the batch from a run containing the smallest so far is completely read finish:
            if (batchTrackers[indexOfSmallest] == inputBuffers.get(indexOfSmallest).capacity()) {
                // reload the next batch of the same run
                Batch nextBatch = readBatch(inputStreams.get(indexOfSmallest));
                if (nextBatch != null) {
                    inputBuffers.set(indexOfSmallest, nextBatch);
                    batchTrackers[indexOfSmallest] = 0; // reset tracker to 0 for new batch
                }
            }

            if (isDistinct) {
                // If it is the same as the last item you added to your output list, throw it away
                // otherwise, add it to your output list
                if (lastTuple != null && comparator.compare(lastTuple, smallest) != 0) {
                    outputBuffer.add(smallest);
                    lastTuple = smallest; // update the last tuple added into the output buffer
                } else if (lastTuple == null) {
                    outputBuffer.add(smallest);
                    lastTuple = smallest;
                } else if (comparator.compare(lastTuple, smallest) == 0) {
                    // Duplicates detected, ignore
                }
            } else {
                outputBuffer.add(smallest);
            }

            // write to file if full
            if (outputBuffer.isFull()) {
                if (output == null) {
                    output = writeRun(Arrays.asList(outputBuffer));
                } else {
                    appendRun(outputBuffer, output);
                }
                outputBuffer.clear();
            }
        }

        if (!outputBuffer.isEmpty()) {
            if (output == null) {
                output = writeRun(Arrays.asList(outputBuffer));
            } else {
                appendRun(outputBuffer, output);
            }
        }

        for (ObjectInputStream ois : inputStreams) {
            closeInputStream(ois);
        }

        return output;
    }

    /**
     * Write run to temporary file
     */
    private File writeRun(List<Batch> run) {
        String fileName = FILE_HEADER + fileNum + passNum + runNum;
        try {
            File file = new File(fileName);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
            for (Batch batch : run) {
                out.writeObject(batch);
            }
            out.close();
            runNum++;
            return file;
        } catch (IOException io) {
            System.out.println("External sort: Writing temp file error");
        }
        return null;
    }

    private void appendRun(Batch run, File dest) {
        try {
            ObjectOutputStream out = new AppendingObjectOutputStream(new FileOutputStream(dest, true));
            out.writeObject(run);
            out.close();
        } catch (IOException e) {
            System.out.println("External sort: error in appending run");
        }
    }

    private Batch readBatch(ObjectInputStream ois) {
        try {
            Batch batch = (Batch) ois.readObject();
            return batch;
        } catch (EOFException e) {
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("ExternalSort: readBatch IOException");
        } catch (ClassNotFoundException e) {
            System.out.println("ExternalSort: readBatch ClassNotFoundException");
        }
        return null;
    }

    /**
     * Clearing up temporary files
     */
    private void clearTempFiles(Vector<File> tempFiles) {
        for (File file : tempFiles) {
            file.delete();
        }
    }

    private Comparator<Tuple> generateComparator() {
        return new SortComparator(table.getSchema());
    }

    // Comparator for DISTINCT
    private Comparator<Tuple> generateComparator(Vector joinAttributes, boolean flag) {
        return new SortComparator(table.getSchema(), joinAttributes, flag);
    }

    class SortComparator implements Comparator<Tuple> {

        private Schema schema;
        private Vector joinAttributes = null;
        private  boolean isDistinct = false;

        SortComparator(Schema schema) {
            this.schema = schema;
        }
        //for distinct
        SortComparator(Schema schema, Vector joinAttributes, boolean flag) {
            this.schema = schema;
            this.joinAttributes = joinAttributes;
            this.isDistinct = flag;
        }

        @Override
        public int compare(Tuple t1, Tuple t2) {
            boolean hasSameAttr = true;
            int finalComparisonResult = 0;
            if (!isDistinct) {
                int joinIndex = schema.indexOf(joinAttribute);
                return Tuple.compareTuples(t1, t2, joinIndex);
            } else {
                Vector attList = joinAttributes;

                for (int i = 0; i < attList.size(); i++) {
                    int index = schema.indexOf((Attribute) attList.get(i));
                    int result = Tuple.compareTuples(t1, t2, index);
                    finalComparisonResult = result;
                    if (result != 0) {
                        hasSameAttr = false;
                        break;
                    }
                }

                // if hasSameAttr == true after going through the comparator the tuple is a duplicate
                // Hence, return 0. Else return the comparison result
                return hasSameAttr ? 0 : finalComparisonResult;
            }
        }
    }

    private void closeInputStream(ObjectInputStream ois) {
        try {
            ois.close();
        } catch (IOException io) {
            System.out.println("ES: IOException error closing input stream");
        }
    }

    public void setDistinct(boolean value) {
        isDistinct = value;
    }
}