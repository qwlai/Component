package qp.operators;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Queue;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {

    int leftIndex, rightIndex;
    int batchSize;
    int lcurs, rcurs;

    boolean eosl;
    boolean eosr;

    String leftFileName;
    String rightFileName;

    Batch rightBatch;   // Buffer for right inputstream
    Batch leftBatch;    // Buffer for left inputstream
    Batch outBatch;     // Output Buffer

    ArrayList<Tuple> rightTuplesEqualsForJoin;
    ArrayList<Tuple> leftTuplesEqualsForJoin;

    PriorityQueue<Integer> leftQueue;
    PriorityQueue<Integer> rightQueue;

    ArrayList<Tuple> leftBlockTuples;

    ObjectInputStream inLeft;

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

        leftQueue = new PriorityQueue<>();
        rightQueue = new PriorityQueue<>();

        ExternalSort leftRelation = new ExternalSort(left, numBuff, leftIndex, "LSRTemp-");
        ExternalSort rightRelation = new ExternalSort(right, numBuff, rightIndex, "RSRTemp-");

        lcurs = 0;
        rcurs = 0;
        eosl = false;
        eosr = true;

        if (!(leftRelation.open() && rightRelation.open())) {
            return false;
        }

        if (!right.open())
            return false;

        if (!left.open())
            return false;

        leftBatch = left.next();
        rightBatch = right.next();

        rightTuplesEqualsForJoin = new ArrayList<>();
        leftTuplesEqualsForJoin = new ArrayList<>();

        return true;
    }

    public Batch next() {

        if (eosl) { // left stream reaches the end
            close();
            return null;
        }
        outBatch = new Batch(batchSize);

        while (!outBatch.isFull()) {
            if (lcurs == 0 && eosr == true) { // left is at the start and right reads until the end
                loadLeftBlock();

                if (leftBlockTuples.size() == 0) {
                    eosl = true;
                    return outBatch;
                }

                try { //Whenever a new left block comes in, we have to start scanning the right page
                    inLeft = new ObjectInputStream(new FileInputStream(leftFileName));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("SortMergeJoin:error in reading the file");
                    System.exit(1);
                }

                int comparison = Tuple.compareTuples(leftBatch.elementAt(lcurs), rightBatch.elementAt(rcurs), leftIndex, rightIndex);
                if (comparison == 0) { //left and right tuples match

                } else if (comparison < 0) { // left tuple < right tuple
                    lcurs++;
                    left.next();
                } else { //left tuple > right tuple
                    rcurs++;
                    right.next();
                }
            }

            loadLeftBlock();
            return outBatch;
        }
    }

    private Tuple pollOutNextTuple(Batch batch) {
        if (batch != null) {
            Tuple tuple = batch.elementAt(0);
            batch.remove(0);
            return tuple;
        } else {
            return null;
        }
    }

    private Tuple pollOutNextLeftTuple() {
        if (leftBatch.isEmpty()) {
            // System.out.println("new left presort");
            leftBatch = left.next();
        }
        return pollOutNextTuple(leftBatch);
    }

    private Tuple pollOutNextRightTuple() {
        if (rightBatch.isEmpty()) {
            // System.out.println("new right presort");
            rightBatch = right.next();
        }
        return pollOutNextTuple(rightBatch);
    }

    // M - 2 buffers used for left table
    private void loadLeftBlock() {
        leftBlockTuples.clear();
        for (int i = 0; i < (numBuff - 2); i++) {
            try {
                Batch batch = (Batch) inLeft.readObject();
                if (batch != null) {
                    // Add the tuples into the blockTuples
                    for (int j = 0; j < batch.size(); j++) {
                        leftBlockTuples.add(batch.elementAt(j));
                    }
                    break;
                }
            } catch (ClassNotFoundException c) {
                System.out.println("SortMergeJoin:Some error in deserialization ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("SortMergeJoin:temporary file reading error");
                System.exit(1);
            }
        }
    }

}
