package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

public class ExternalSort extends Operator {

    private Operator table;
    private Stack fileStack; // for file deletion
    private ArrayList<Tuple> mainMemTuples;

    private boolean phaseTwoFlag; // flag to indicate phase 2

    private String fileName;
    private int numBuff, runNum, joinIndex, batchSize;

    private Batch batch;

    private ObjectInputStream in;
    private Vector projectList;
    private boolean isDistinct;
    private Vector attributeList;
    private Tuple prevTuple;

    public ExternalSort(Operator table, int numBuff, int joinIndex, String fileName) {
        super(OpType.SORT);
        this.table = table;
        this.numBuff = numBuff;
        this.joinIndex = joinIndex;
        this.fileName = fileName;
    }

//    public ExternalSort(Operator table, int numBuff, int joinIndex, String fileName, boolean isDistinct, Vector projectList) {
//        super(OpType.SORT);
//        this.table = table;
//        this.numBuff = numBuff;
//        this.joinIndex = joinIndex;
//        this.fileName = fileName;
//        this.projectList = projectList;
//        this.isDistinct = isDistinct;
//    }

    public boolean open() {
        if (!table.open()) {
            return false;
        }
        fileStack = new Stack();
        mainMemTuples = new ArrayList<>();
        generateSortedRuns();

        phaseTwoFlag = true;
        mergeSortedRuns();

        readSortedRun(0);
        System.out.println(mainMemTuples.size());
        return true;
    }

    /**
     * Generate all the sorted runs
     */
    /**
     * Generate all the sorted runs
     */
    private void generateSortedRuns() {

        /** Initializing for first run **/
        if (runNum == 0) {
            batch = table.next();
            batchSize = batch.size();
        }

        while (batch != null) {
            loadTuplesIntoMainMem();
            if (batch == null && mainMemTuples.size() == 0) { // No more pages left to be read
                break;
            }
            sortMainMem();
            writeSortedRuns(runNum);
            runNum++;
        }
    }


    /**
     * Loads the tuples in the pages into main memory
     * Pages to load = number of buffer
     */
    private void loadTuplesIntoMainMem() {
        for (int i = 0; i < numBuff; i++) {
            if (batch != null) {
                for (int j = 0; j < batch.size(); j++) {
                    mainMemTuples.add(batch.elementAt(j));
                }
            } else {
                return;
            }
            batch = table.next();
        }
    }


    /**
     * Sorts main meomory tuples
     * Phase 2 duplicate elimination
     * but we are unable to implement
     */
    private void sortMainMem() {
//        System.out.println("SORT---------------");
//        if (isDistinct && attributeList != null) {
//
//            Collections.sort(mainMemTuples, new Comparator<Tuple>() {
//                @Override
//                public int compare(Tuple o1, Tuple o2) {
//                    //Vector attList = attributeList;
//
//                    int finalComparison = 0;
//                    for (int i = 0; i < o1._data.size(); i++) {
//                        //int index = table.getSchema().indexOf((Attribute) attList.get(i));
//                        int result;
//                        result = Tuple.compareTuples(o1, o2, i);
//                        finalComparison = result;
//                        if (result != 0) {
//                            break;
//                        }
//                    }
//                    return finalComparison;
//                }
//            });
//
//            System.out.println("after sort---------------");
//            for (Tuple t : mainMemTuples) {
//                Debug.PPrint(t);
//            }
//            System.out.println("after sort---------------");
//
//
//            Tuple compareThis = null;
//            boolean ifAllsame = false;
//            System.out.println("mainmemsize-----------" + mainMemTuples.size());
//            ArrayList<Tuple> tuplesToBeRemoved = new ArrayList<>();
//            for (int i = 0; i < mainMemTuples.size(); i++) {
//                for (int j = 0; j < mainMemTuples.get(i)._data.size(); j++) {
//                    if (compareThis == null) {
//                        System.out.println("null once");
//                        compareThis = mainMemTuples.get(i);
//                        break;
//                    } else {
//                        if (Tuple.compareTuples(compareThis, mainMemTuples.get(i), j) == 0) {
//                            ifAllsame = true;
//                        }
//                        else {
//                            ifAllsame = false;
//                            compareThis = mainMemTuples.get(i);
//                            break;
//                        }
//
//                    }
//                }
//                if (ifAllsame) {
//                    tuplesToBeRemoved.add(mainMemTuples.get(i));
//                    System.out.println("DUPLICATE");
//                }
//            }
//            for (int i = 0; i < tuplesToBeRemoved.size(); i++) {
//                mainMemTuples.remove(tuplesToBeRemoved.get(i));
//            }
//
//            System.out.println("after remove duplicate---------------");
//            for (Tuple t : mainMemTuples) {
//                Debug.PPrint(t);
//            }
//            System.out.println("after remove duplicate---------------");
//
//        } else {
            Collections.sort(mainMemTuples, ((t1, t2) -> Tuple.compareTuples(t1, t2, joinIndex)));
//        }
    }

    /**
     * Write sorted runs batch by batch
     * Tried to implement phase 1 attribute selection
     */
    private void writeSortedRuns(int currentRun) {
        if (!phaseTwoFlag)
            fileStack.push(currentRun);
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileName + currentRun));

            /** There are still tuples in main memory */
            while (!mainMemTuples.isEmpty()) {
                Batch b = new Batch(batchSize);

                /** Buffer is not full, add tuples to the batch */
                while (!b.isFull()) {
                    if (mainMemTuples.isEmpty()) { // All tuples have been put into a batch
                        break;
                    }

//                    // phase 1 write attributes that we are interested into file
//                    if (isDistinct && !phaseTwoFlag) {
//                        Tuple outTuple;
//                        Vector present = new Vector();
//
//                        // Add join Index at first element
//                        present.add(mainMemTuples.get(0).dataAt(joinIndex));
//
//                        // Projection index
//                        for (int i = 0; i < table.getSchema().getAttList().size(); i++) {
//                            for (int j = 0; j < projectList.size(); j++) {
//                                if (table.getSchema().getAttribute(i).equals((Attribute) projectList.get(j))) {
//                                    if (joinIndex != i) {// add those indexes that are projected except join index
//                                        present.add(mainMemTuples.get(0).dataAt(i));
//                                        attributeList.add(mainMemTuples.get(0).dataAt(i));
//                                    }
//                                }
//                            }
//                        }
//
//                        outTuple = new Tuple(present);
//                        b.add(outTuple);
//                        mainMemTuples.remove(0);
//                    } else {
                        b.add(mainMemTuples.get(0));
                        mainMemTuples.remove(0);
//                    }
                }
                out.writeObject(b);
            }
            out.close();
        } catch (IOException io) {
            System.err.println("ExternalSort: error in writing file");
            System.exit(1);
        }
    }

    /** Read the all pages in the run */
    private boolean readSortedRun(int currentRun) {

        try {
            in = new ObjectInputStream(new FileInputStream(fileName + currentRun));
            Batch inBatch;
            while ((inBatch = (Batch) in.readObject()) != null) {
                for (int i = 0; i < inBatch.size(); i++) {
                    /** Add tuples of pages into memory */
                    mainMemTuples.add(inBatch.elementAt(i));
                }
            }
        } catch (EOFException e) {
            try {
                in.close();
                return true;
            } catch (IOException io) {
                System.exit(1);
            }
        } catch (ClassNotFoundException e) {
            System.out.println("ExternalSort:Some error in deserialization ");
            System.exit(1);
        } catch (IOException io) {
            return false;

        }
        return true;
    }

    /**
     * Phase 2: Merge sorted runs
     * Pages to merge = numBuffer - 1;
     */
    private void mergeSortedRuns() {
        int numOfUsableBuffers = numBuff - 1;
        int readInCurrentRun = 0;
        int writeOutRunCounter = 0;

        while (runNum != 1) { // last run not completed yet

            /** Merge all runs in current pass */
            while (readInCurrentRun != runNum) {
                for (int i = 0; i < numOfUsableBuffers; i++) {
                    if (!readSortedRun(readInCurrentRun)) {
                        break;
                    }
                    readInCurrentRun++;
                }
                sortMainMem();
                writeSortedRuns(writeOutRunCounter);
                writeOutRunCounter++;
            }
            readInCurrentRun = 0;
            writeOutRunCounter = 0;

            /** Number of sorted runs left */
            runNum = (int) Math.ceil((double) runNum / numOfUsableBuffers);
            close();
        }
    }

    /**
     * Close the operator, delete all temp files
     */
    public boolean close() {
        while((int) fileStack.peek() != runNum - 1) {
            File f = new File(fileName + (int) fileStack.peek());
            f.delete();
            fileStack.pop();
        }
        return true;
    }
}

