package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    int batchSize; // number of tuples per out batch

    /** The following fields are useful during execution of the BlockNestedJoin operation */
    int leftIndex; // Index of the join attribute in left table
    int rightIndex; // Index of the join attribute in right table

    String rightFileName; // The file name where the right table is materialize

    static int fileNum = 0; // To get unique file Number for this operation

    Batch outBatch; // Output buffer

    ArrayList<Tuple> leftBlockTuples;

    Batch rightBatch; // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs; // Cursor for left side buffer
    int rcurs; // Cursor for right side buffer

    boolean eosl; // End of stream (left table) is reached
    boolean eosr; // End of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();

    }

    /**
     * Find the indexes of the join attributes
     */
    public boolean open() {

        leftBlockTuples = new ArrayList<>();

        /** Select number of tuples per batch **/
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        /** Get left and right attribute **/
        Attribute leftAttr = con.getLhs();
        Attribute rightAttr = (Attribute) con.getRhs();

        leftIndex = left.getSchema().indexOf(leftAttr);
        rightIndex = right.getSchema().indexOf(rightAttr);

        Batch rightPage;

        /** Initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;

        /**
         * Right stream is to be repetitively scanned, if it reached the end, we have to start new scan
         */
        eosr = true;

        if (!right.open()) {
            return false;
        } else {
            fileNum++;
            rightFileName = "BNJtemp-" + String.valueOf(fileNum);

            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rightFileName));
                while ((rightPage = right.next()) != null) { //output
                    out.writeObject(rightPage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin:writing the temporary file error");
                return false;
            }
            if (!right.close()) return false;
        }
        if (left.open()) return true;
        else return false;
    }

    /**
     * Load input buffers with M-2 pages for pages from left table
     * 1 buffer for 1 page of right table
     * 1 buffer for output table
     * Returns a page of output tuples
     */
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
                    in = new ObjectInputStream(new FileInputStream(rightFileName));
                    eosr = false;

                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }
            }

            // Got data to read from right batch
            while (eosr == false) {

                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightBatch = (Batch) in.readObject();
                    }

                    for (int i = lcurs; i < leftBlockTuples.size(); i++) {
                        for (int j = rcurs; j < rightBatch.size(); j++) {
                            Tuple leftTuple = leftBlockTuples.get(i);
                            Tuple rightTuple = rightBatch.elementAt(j);

                            // if left and right index matches, add the tuple to outbatch
                            if (leftTuple.checkJoin(rightTuple, leftIndex, rightIndex)) {
                                Tuple outTuple = leftTuple.joinWith(rightTuple);

                                outBatch.add(outTuple);
                                if (outBatch.isFull()) {
                                    if (i == leftBlockTuples.size() - 1 && j == rightBatch.size() - 1) {
                                        // case 1 : reaches end of left block and right page; reset cursors
                                        lcurs = 0;
                                        rcurs = 0;
                                    } else if (i != leftBlockTuples.size() - 1 && j == rightBatch.size() - 1) {
                                        // case 2 : reaches the end for right page, point the lcurs to the next tuple
                                        lcurs = i + 1;
                                        rcurs = 0;
                                    } else {
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }
                                    return outBatch;
                                }
                            }
                        }
                        rcurs = 0;
                    }
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin:Error in temporary file reading");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin:Some error in deserialization ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin:temporary file reading error");
                    System.exit(1);
                }
            }
        }
        return outBatch;
    }

    /** Close the operator */
    public boolean close() {
        File f = new File(rightFileName);
        f.delete();
        return true;
    }

    // M - 2 buffers used for outer table
    private void loadLeftBlock() {
        leftBlockTuples.clear();
        for (int i = 0; i < (numBuff - 2); i++) {
            Batch batch = left.next();
            if (batch != null) {
                // Add the tuples into the blockTuples
                for (int j = 0; j < batch.size(); j++) {
                    leftBlockTuples.add(batch.elementAt(j));
                }
            }
        }
    }
}
