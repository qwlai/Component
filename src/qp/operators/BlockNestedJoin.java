package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class BlockNestedJoin extends Join {

    private int BLOCKSIZE; // Number of tuples per out block

    /**
     * The following fields are useful during execution of * the NestedJoin operation
     */
    private int LEFTINDEX; // Index of the join attribute in left table

    private int RIGHTINDEX; // Index of the join attribute in right table

    private String rightFileName; // The file name where the right table is materialized

    private static int filenum = 0; // To get unique filenum for this operation

    private Batch outblock; // Output buffer
    private Batch rightblock; // Buffer for right input stream
    private ObjectInputStream in; // File pointer to the right hand materialized file

    private List<Tuple> leftBlockTuple;

    private int lcurs; // Cursor for left side buffer
    private int rcurs; // Cursor for right side buffer
    private boolean eosl; // Whether end of stream (left table) is reached
    private boolean eosr; // End of stream (right table)

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open find the index of the join attributes
     * Materializes the right hand side into a file
     * Opens the connections
     */
    public boolean open() {

        leftBlockTuple = new ArrayList<>();

        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        BLOCKSIZE = Batch.getPageSize() / tuplesize;

        /** get left and right attribute from condition */
        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();

        /** */
        LEFTINDEX = left.getSchema().indexOf(leftattr);
        RIGHTINDEX = right.getSchema().indexOf(rightattr);
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;

        /**
         * because right stream is to be repetitively scanned * if it reached end, we have to start new scan
         */
        eosr = true;

        /** Right hand side table is to be materialized * for the Nested join to perform */
        if (!right.open()) {
            return false;
        } else {

            /**
             * If the right operator is not a base table then * Materialize the intermediate result from
             * right * into a file
             */
            // if(right.getOpType() != OpType.SCAN){
            filenum++;
            rightFileName = "NJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rightFileName));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin:writing the temporary file error");
                return false;
            }
            // }
            if (!right.close()) return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition * And returns a page of output tuples
     */
    public Batch next() {
        // System.out.print("BlockNestedJoin:--------------------------in next----------------");
        // Debug.PPrint(con);
        // System.out.println();
        int i, j;
        if (eosl) {
            close();
            return null;
        }
        outblock = new Batch(BLOCKSIZE);

        while (!outblock.isFull()) {
            //at the start lcurs is 0 and eosr is true
            if (lcurs == 0 && eosr == true) {
                /** new left block is to be fetched* */
                loadLeftBlock();

                if (leftBlockTuple.size() == 0) { //no more left pages
                    eosl = true;
                    return outblock;    //returns output block
                }
                /** Whenever a new left block comes, we have to start the * scanning of right table */
                try {
                    in = new ObjectInputStream(new FileInputStream(rightFileName));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }
            }

            while (eosr == false) { // still have data to continue to read from larger table(the one that is not in hash table)
                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightblock = (Batch) in.readObject();
                    }

                    for (i = lcurs; i < leftBlockTuple.size(); i++) {
                        for (j = rcurs; j < rightblock.size(); j++) {
                            Tuple leftTuple = leftBlockTuple.get(i);
                            Tuple rightTuple = rightblock.elementAt(j);
                            if (leftTuple.checkJoin(rightTuple, LEFTINDEX, RIGHTINDEX)) {
                                Tuple outtuple = leftTuple.joinWith(rightTuple);

                                outblock.add(outtuple);
                                if (outblock.isFull()) {
                                    if (i == leftBlockTuple.size() - 1 && j == rightblock.size() - 1) { // case 1 left and right both nothng to scan, so reset cursors
                                        lcurs = 0;
                                        rcurs = 0;
                                    } else if (i != leftBlockTuple.size() - 1 && j == rightblock.size() - 1) { // case 2 left still have things to scan, right nothing to scan, so reset right
                                        lcurs = i + 1;
                                        rcurs = 0;
                                    } else if (i == leftBlockTuple.size() - 1 && j != rightblock.size() - 1) { // case 3 left reach last tuple, but right still jhave some tuples to scan, so
                                        lcurs = i;
                                        rcurs = j + 1;
                                    } else {
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }
                                    return outblock;
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
        return outblock;
    }

    //
    public void loadLeftBlock() {
        leftBlockTuple.clear();

        for (int i = 0; i < (numBuff - 2); i++) {
            Batch block = left.next();

            if (block != null) {
                for (int j = 0; j < block.size(); j++) {
                    leftBlockTuple.add(block.elementAt(j));
                }
            }
        }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rightFileName);
        f.delete();
        return true;
    }
}
