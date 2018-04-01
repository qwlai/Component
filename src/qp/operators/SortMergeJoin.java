package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;

public class SortMergeJoin extends Join {

    int leftIndex, rightIndex;
    int batchSize;

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

        ExternalSort leftRelation = new ExternalSort(left, numBuff, leftIndex, "LSRTemp-");
        ExternalSort rightRelation = new ExternalSort(right, numBuff, rightIndex, "RSRTemp-");

        if (!(leftRelation.open() && rightRelation.open())) {
            return false;
        }
        return true;
    }



}