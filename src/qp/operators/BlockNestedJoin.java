package qp.operators;

import static java.lang.System.exit;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class BlockNestedJoin extends Join {

    int batchsize;  //Number of tuples per out batch

    /**
     * The following fields are useful during execution of
     * * the NestedJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String rfname;    // The file name where the right table is materialize

    static int filenum = 0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    List<Batch> leftbatches = new LinkedList<>();  // List of Buffer for left input stream
    ArrayList<Tuple> leftTuples = new ArrayList<>();

    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer
    boolean eosl;  // Whether end of stream (left table) is reached
    boolean eosr;  // End of stream (right table)

    public BlockNestedJoin(Join jn) {
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
        leftbatches = new LinkedList<>(); //initialise new leftbatches for every call

        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

        /** Reset the cursors of input buffers **/

        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        Batch rightpage;

        if (materializeRightTable()) return false;
        if (left.open())
            return true;
        else
            return false;
    }

    private boolean materializeRightTable() {
        Batch rightpage;
        if (!right.open()) {
            return true;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/

            //if(right.getOpType() != OpType.SCAN){
            int id = filenum;
            filenum++;
            rfname = "NJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin:writing the temporary file error");
                return true;
            }
            //}
            if (!right.close())
                return true;
        }
        return false;
    }


    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/


    public Batch next() {
        //System.out.print("NestedJoin:--------------------------in next----------------");
        //Debug.PPrint(con);
        //System.out.println();
        int i, j;
        if (eosl && leftbatches.isEmpty()) {
            close();
            return null;
        }
        Batch outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {

            if (lcurs == 0 && eosr) {
                //Load all the leftbatches
                /** new left page is to be fetched**/
                //Clear any existing data
                this.leftbatches.clear();
                this.leftTuples.clear();
                //load leftbatches with data
                loadLeftBatches();
                //Load data from batch to leftTuples
                loadTuplesFromBatch();
                //if batch is empty reinitialise right materialized stream
                reinitialiseRightMaterializedStream();
                if (leftbatches.size() == 0) {
                    eosl = true;
                    return outbatch;
                }
            }

            while (eosr == false) { //iterate right buffer all the way

                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }

                    for (i = lcurs; i < leftTuples.size(); i++) {
                        for (j = rcurs; j < rightbatch.size(); j++) {
                            Tuple lefttuple = leftTuples.get(i);
                            Tuple righttuple = rightbatch.elementAt(j);
                            if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                Tuple outtuple = lefttuple.joinWith(righttuple);

                                //Debug.PPrint(outtuple);
                                //System.out.println();
                                outbatch.add(outtuple);
                                if (outbatchFull(i, j, outbatch)) return outbatch;
                            }
                        }
                        rcurs = 0;
                    }
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("NestedJoin:Error in temporary file reading");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("NestedJoin:Some error in deserialization ");
                    exit(1);
                } catch (IOException io) {
                    System.out.println("NestedJoin:temporary file reading error");
                    exit(1);
                }
            }
        }
        return outbatch;
    }

    private boolean outbatchFull(int i, int j, Batch outbatch) {
        if (outbatch.isFull()) {
            if (i == leftTuples.size() - 1 && j == rightbatch.size() - 1) {//case 1 both left and right batch done
                lcurs = 0;
                rcurs = 0;
            } else if (i != leftTuples.size() - 1 && j == rightbatch.size() - 1) {//case 2 right batch done
                lcurs = i + 1;
                rcurs = 0;
            } else if (i == leftTuples.size() - 1 && j != rightbatch.size() - 1) {//case 3 next tuple in right batch
                lcurs = i;
                rcurs = j + 1;
            } else {
                lcurs = i;
                rcurs = j + 1;
            }
            return true;
        }
        return false;
    }

    private void reinitialiseRightMaterializedStream() {
        if (!(leftbatches.size() == 0)) {
            try {
                FileInputStream fis = new FileInputStream(rfname);
                InputStream buff = new BufferedInputStream(fis);
                in = new ObjectInputStream(buff);
                eosr = false;
            } catch (Exception e) {
                System.out.println("Error in block nested loop in reading of data");
                exit(1);
            }
        }
    }

    private void loadTuplesFromBatch() {
        for (int m = 0; m < leftbatches.size(); m++) {
            Batch batch = leftbatches.get(m);
            for (int n = 0; n < batch.size(); n++)
                leftTuples.add(batch.elementAt(n));
        }
    }

    private void loadLeftBatches() {
        for (int m = 0; m < (numBuff - 2); m++) {
            Batch batch = left.next(); // get next batch of data
            if (batch != null) leftbatches.add(batch);
        }
    }

    /** Close the operator */
    /**public boolean close(){

     File f = new File(rfname);
     f.delete();
     return true;

     }
     */
}
