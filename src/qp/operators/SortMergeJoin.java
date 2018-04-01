/**
 * page nested join algorithm
 **/

package qp.operators;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {


    private int batchsize;  //Number of tuples per out batch

    /**
     * The following fields are useful during execution of
     * * the NestedJoin operation
     **/
    private int leftindex;     // Index of the join attribute in left table
    private int rightindex;    // Index of the join attribute in right table

    private String rfname;    // The file name where the right table is materialize

    private static int filenum = 0;   // To get unique filenum for this operation

    private Batch outbatch;   // Output buffer
    private Batch leftbatch;  // Buffer for left input stream
    private Batch rightbatch;  // Buffer for right input stream
    private Vector<Batch> leftbatches;

    private ObjectInputStream in;

    private int k = 0;

    private int lcurs;    // Cursor for left side buffer
    private int rcurs;    // Cursor for right side buffer
    private int leftBatchIndex;
    private int rightBatchIndex;
    private int leftBlockIndex;
    private int maxLeftBlocks; // Total size of group of left side buffers

    private int leftBufferSize;

    private ExternalSort leftSort;
    private ExternalSort rightSort;

    private List<File> sortedLeftFiles;
    private List<File> sortedRightFiles;

    private final String SORTED_LEFT_FILE_NAME = "SMJ-Left";
    private final String SORTED_RIGHT_FILE_NAME = "SMJ-Right";

    private boolean hasMatch;
    private int rightFirstMatchIndex;
    private int rightFirstMatchBatchIndex;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /**
     * During open finds the index of the join attributes
     * *  Materializes the right hand side into a file
     * *  Opens the connections
     **/


    public boolean open() {

        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

        // Sorts both left and right relations
        leftSort = new ExternalSort(left, leftattr, numBuff, false);
        rightSort = new ExternalSort(right, rightattr, numBuff, false);

        if (!(leftSort.open() && (rightSort.open()))) {
            return false;
        }

        try {
            sortedLeftFiles = writeSortedFiles(leftSort, SORTED_LEFT_FILE_NAME);
            sortedRightFiles = writeSortedFiles(rightSort, SORTED_RIGHT_FILE_NAME);
        } catch (IOException io) {
            System.out.println("SortMergeJoin: Error in writing sorted files");
            return false;
        }

        leftSort.close();
        rightSort.close();

        hasMatch = false;
        rightFirstMatchIndex = 0;
        rightFirstMatchBatchIndex = 0;

        leftBufferSize = numBuff - 2; // 1 buffer for right, 1 for output, remaining for left
        // right will probe left

        /** initialize the cursors of input buffers **/

        lcurs = 0;
        rcurs = 0;
        leftBatchIndex = 0;
        leftBlockIndex = 0;
        rightBatchIndex = 0;
        maxLeftBlocks = (int) Math.ceil(sortedLeftFiles.size() / (double) leftBufferSize);

        try {
            leftbatches = getNextLeftBuffers();
            if (leftbatches == null) {
                return false;
            }
            leftbatch = leftbatches.get(0);
            rightbatch = getRightBuffer();
        } catch (NullPointerException npe) {
            System.out.println("SMJ: Cannot perform join");
        } catch (ArrayIndexOutOfBoundsException aiooe) {
            return false;
        }

        return true;
    }


    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/


    public Batch next() {
        //System.out.print("NestedJoin:--------------------------in next----------------");
        //Debug.PPrint(con);
        //System.out.println();
        if (leftBlockIndex == maxLeftBlocks || leftbatch == null || rightbatch == null) {
            close();
            return null;
        }
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {

            if (lcurs == leftbatch.size()) {
                leftBatchIndex++;
                if (leftBatchIndex < leftbatches.size()) {
                    leftbatch = leftbatches.get(leftBatchIndex);
                } else {
                    leftBatchIndex = 0;
                    leftBlockIndex++;
                    leftbatches = getNextLeftBuffers();
                    if (leftbatches == null) {
                        close();
                        return outbatch.isEmpty() ? null : outbatch;
                    } else {
                        leftbatch = leftbatches.get(leftBatchIndex);
                    }
                }
                lcurs = 0;
            }

            if (rcurs == rightbatch.size()) {
                rightBatchIndex++;
                rightbatch = getRightBuffer();
                if (rightbatch == null) {
                    close();
                    return outbatch.isEmpty() ? null : outbatch;
                }
                rcurs = 0;
            }

            while (lcurs < leftbatch.size() && rcurs < rightbatch.size()) {
                Tuple leftTuple = leftbatch.elementAt(lcurs);
                Tuple rightTuple = rightbatch.elementAt(rcurs);
                int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
                if (comparison < 0) {  // left tuple < right tuple
                    lcurs++;  // move to next left tuple
                    if (hasMatch) {
                        rightFirstMatchIndex = rcurs;
                        rightFirstMatchBatchIndex = rightBatchIndex;
                    }
                    hasMatch = false;
                } else if (comparison > 0) {  // left tuple > right tuple
                    rcurs++;  // move to next right tuple
                    hasMatch = false;
                } else { // match
                    if (!hasMatch) {
                        rcurs = rightFirstMatchIndex;
                        if (rightBatchIndex > rightFirstMatchBatchIndex) {
                            rightBatchIndex = rightFirstMatchBatchIndex;
                            rightbatch = getRightBuffer(rightFirstMatchBatchIndex);
                        }
                        hasMatch = true;
                    }
                    Tuple joinTuple = leftTuple.joinWith(rightTuple);
                    outbatch.add(joinTuple);
                    rcurs++;
                }
                if (outbatch.isFull()) {
                    return outbatch;
                }
            }
        }
        return outbatch.isEmpty() ? null : outbatch;
    }


    /**
     * Close the operator
     */
    public boolean close() {

        //clearTempFiles(sortedLeftFiles);
       // clearTempFiles(sortedRightFiles);

        return true;

    }


    private Vector<Batch> getNextLeftBuffers() {
        if (leftBlockIndex == maxLeftBlocks) {
            return null;
        }

        int start = leftBlockIndex * leftBufferSize;
        int end = (leftBlockIndex + 1) * leftBufferSize;
        if (end > sortedLeftFiles.size()) {
            end = sortedLeftFiles.size();
        }

        Vector<Batch> batchList = new Vector<>();
        for (int i = start; i < end; i++) {
            File file = sortedLeftFiles.get(i);
            Batch batch;
            try {
                in = new ObjectInputStream(new FileInputStream(file));
                batch = (Batch) in.readObject();
                in.close();
                batchList.add(batch);
            } catch (IOException io) {
                System.out.println("SortMergeJoin: IOException in reading sortedLeftFiles");
                return null;
            } catch (ClassNotFoundException cnfe) {
                System.out.println("SortMergeJoin: ClassNotFoundException in deserialization");
            }
        }
        return batchList;
    }


    private Batch getRightBuffer() {
        if (rightBatchIndex == sortedRightFiles.size()) {
            return null;
        }
        Batch batch = null;
        File file = sortedRightFiles.get(rightBatchIndex);
        try {
            in = new ObjectInputStream(new FileInputStream(file));
            batch = (Batch) in.readObject();
            in.close();
        } catch (IOException ioe) {
            System.out.println("SortMergeJoin: IOException in reading sortedRightFiles at next()");
        } catch (ClassNotFoundException cnfe) {
            System.out.println("SortMergeJoin: ClassNotFoundException in deserialization");
        }
        return batch;
    }

    private Batch getRightBuffer(int index) {
        Batch batch = null;
        File file = sortedRightFiles.get(index);
        try {
            in = new ObjectInputStream(new FileInputStream(file));
            batch = (Batch) in.readObject();
            in.close();
        } catch (IOException ioe) {
            System.out.println("SortMergeJoin: IOException in reading sortedRightFiles at next()");
        } catch (ClassNotFoundException cnfe) {
            System.out.println("SortMergeJoin: ClassNotFoundException in deserialization");
        }
        rightBatchIndex++;
        return batch;
    }



    private List<File> writeSortedFiles(Operator op, String filePrefix) throws IOException {
        Batch batch;
        int num = 0;
        List<File> files = new Vector<>();
        while ((batch = op.next()) != null) {
            File file = new File(filePrefix + num);
            num++;
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
            out.writeObject(batch);
            files.add(file);
            out.close();
        }
        return files;
    }


    /**
     * Clearing up temporary files
     */
/**    private void clearTempFiles(List<File> files) {
        for (File file : files) {
            file.delete();
        }
    }
*/

}
