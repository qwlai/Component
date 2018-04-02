package qp.operators;

import java.io.*;
import java.util.PriorityQueue;
import java.util.Stack;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {

    private int batchSize; // number of tuples per out batch

    /** The following fields are useful during execution of the SortMergeJoin operation */
    private int leftIndex; // Index of the join attribute in left table
    private int rightIndex; // Index of the join attribute in right table

    private Batch outBatch; // Output buffer

    private ObjectInputStream inLeft; // File pointer to the left sorted run
    private ObjectInputStream inRight; // File pointer to the right sorted run

    private static String leftRunName = "LSRTemp-";
    private static String rightRunName = "RSRTemp-";

    private ExternalSort leftRelation;
    private ExternalSort rightRelation;

    private PriorityQueue<Tuple> leftPQ;
    private PriorityQueue<Tuple> rightPQ;
    private Stack<Tuple> tupleStack; // Stack to store deleted values for duplicate handling

    private boolean hasLoadLastLeftBlock;
    private boolean hasLoadLastRightBatch;

    private boolean hasFinishLeftRelation;
    private boolean hasFinishRightRelation;

    private boolean isFirstBatch = true;
    private boolean isFirstBlock = true;

    private Tuple leftTuple = null;
    private Tuple rightTuple = null;


    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    public boolean open() {

        /** Number of tuples per batch **/
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        /** Get left and right attribute **/
        Attribute leftAttr = con.getLhs();
        Attribute rightAttr = (Attribute) con.getRhs();

        leftIndex = left.getSchema().indexOf(leftAttr);
        rightIndex = right.getSchema().indexOf(rightAttr);

        leftRelation = new ExternalSort(left, numBuff, leftIndex, leftRunName);
        rightRelation = new ExternalSort(right, numBuff, rightIndex, rightRunName);

        if (!(leftRelation.open() && rightRelation.open())) {
            return false;
        }

        try {
            inLeft = new ObjectInputStream(new FileInputStream(leftRunName + "0"));
            inRight = new ObjectInputStream(new FileInputStream(rightRunName + "0"));
        } catch (IOException io) {
            System.out.println("SortMergeJoin::Error reading in files from external sort");
            System.exit(1);
        }

        leftPQ = new PriorityQueue<>((t1,t2) -> Tuple.compareTuples(t1, t2, leftIndex));
        rightPQ = new PriorityQueue<>((t1,t2) -> Tuple.compareTuples(t1, t2, rightIndex));
        tupleStack = new Stack<>();

        return true;
    }

    /**
     * Load input buffers with M-2 pages for pages from left relation
     * 1 buffer for 1 page of right relation
     * 1 buffer for output table
     * Returns a page of output tuples
     */
    public Batch next() {
        outBatch = new Batch(batchSize);

        if (hasFinishRightRelation || hasFinishLeftRelation) {
            return null;
        }

        while (!outBatch.isFull()) { // output batch is not full

            if (!hasLoadLastLeftBlock) {
                loadLeftBlock(); // load blocks from left relation
            }

            while (!leftPQ.isEmpty()) { // Once leftPQ is empty, loads the next left block
                processRightRelation(false);

                /**
                 * Last element of right relation reached, repeatedly exhaust left relation
                 * until right progresses to null element, check for duplicate as well
                 */
                if (rightPQ.isEmpty() && hasLoadLastRightBatch) {
                    while (true) {
                        compareWithRightRelation();
                        if (rightTuple == null) {
                            while (Tuple.compareTuples(leftPQ.peek(), leftTuple, leftIndex) == 0) {
                                leftTuple = leftPQ.poll();
                                undoPQ();
                                processRightRelation(true);
                                compareWithRightRelation(); // handle last right tuple
                                if (leftPQ.peek() == null) { // no more duplicates found
                                    break;
                                }
                            }
                            hasFinishRightRelation = true;
                            return outBatch;
                        }
                    }
                }

                if (outBatch.isFull()) {
                    return outBatch;
                }
            }

            /**
             * Last element of left relation reached, repeatedly exhaust right relation
             * until left progresses to null element
             */
            if (leftPQ.isEmpty() && hasLoadLastLeftBlock) {
                while (true) {
                    if (rightTuple != null) {
                        processRightRelation(false);
                    }
                    hasFinishLeftRelation = true;
                    return outBatch;
                }
            }
        }
        return outBatch;
    }

    /**
     * Loads left block, M - 2 buffer used for left block
     * @Exception EOFException when no more batch object to be read
     */
    private void loadLeftBlock() {
        for (int i = 0; i < (numBuff - 2); i++) {
            try {
                Batch batch = (Batch) inLeft.readObject();

                if (batch != null) {
                    for (int j = 0; j < batch.size(); j++) {
                        leftPQ.add(batch.elementAt(j));
                    }
                }
            } catch (EOFException e) {
                try { // 1 load all into buffer
                    if (isFirstBlock) {
                        leftTuple = leftPQ.poll();
                        isFirstBlock = false;
                    }

                    inLeft.close();
                    hasLoadLastLeftBlock = true;
                    File f = new File(leftRunName + "0");
                    f.delete();
                } catch (IOException io) {
                    System.out.println("SortMergeJoin: Error in temp read");
                    System.exit(1);
                }
            } catch (ClassNotFoundException cnfe) {
                System.out.println("SortMergeJoin: Some error in deserialization.");
                System.exit(1);
            } catch (IOException io) {
                return;
            }
        }

        if (isFirstBlock) {
            leftTuple = leftPQ.poll();
            isFirstBlock = false;
        }
    }

    /**
     * Process right relation, read right batch when rightPQ is empty
     */
    private void processRightRelation(boolean hasDuplicate) {
        while (!rightPQ.isEmpty()) {
            if (!hasDuplicate) {
                if (leftPQ.isEmpty()) {
                    return;
                }
            }
            compareWithRightRelation();
        }
        readRightBatch();
    }

    /**
     * Compare left and right tuples, join tuples if they match the condition
     */
    private void compareWithRightRelation() {

        int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftIndex, rightIndex);

        if (comparison == 0) { // matching join value, poll right
            Tuple outTuple = leftTuple.joinWith(rightTuple);

            outBatch.add(outTuple);
            tupleStack.push(rightTuple);
            rightTuple = rightPQ.poll();

            if (outBatch.isFull()) { // after adding, check if is full
                return;
            }
        } else if (comparison > 0) { // left > right, progress right
            rightTuple = rightPQ.poll();
        } else {
            if (Tuple.compareTuples(leftPQ.peek(), leftTuple, leftIndex) == 0) { // left < right progress left

                leftTuple = leftPQ.poll();
                undoPQ();
                compareWithRightRelation();
                processRightRelation(true);
            } else {
                leftTuple = leftPQ.poll();
            }
        }
    }

    /** Reads 1 batch file from right relation */
    private void readRightBatch() {
        try {
            Batch rightBatch = (Batch) inRight.readObject();

            for (int i = 0; i < rightBatch.size(); i++) { // add tuples from right batch into right PQ
                rightPQ.add(rightBatch.elementAt(i));
            }

            if (isFirstBatch) {
                rightTuple = rightPQ.poll();
                isFirstBatch = false;
            }

        } catch (EOFException eof) { // Right relation has read the end
            try {
                inRight.close();
                hasLoadLastRightBatch = true;
                File f = new File(rightRunName + "0");
                f.delete();
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error in temp read");
                System.exit(1);
            }
        } catch (ClassNotFoundException cnfe) {
            System.out.println("SortMergeJoin:Error in deserializing");
            System.exit(1);
        } catch (IOException io) {
            return;
        }
    }

    /** Handle case where join condition is not set on pkey, duplicates may occur on left relation
     *  Undo PQ to previous state
     */
    private void undoPQ() {
        while (true) {
            /** Retrace right tuples until it goes back to the first occurence of the match */
            if (Tuple.compareTuples(leftTuple, tupleStack.peek(), leftIndex, rightIndex) == 0) {
                rightPQ.add(tupleStack.pop());
            } else {
                rightTuple = rightPQ.poll();
                return;
            }
        }
    }

    public boolean close() {
        return (left.close() && right.close());
    }
}
