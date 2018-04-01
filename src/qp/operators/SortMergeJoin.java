package qp.operators;

import qp.utils.Attribute;

public class SortMergeJoin extends Join {

    int leftIndex;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    public boolean open() {
        System.out.println("-------------------------------------------");
        Attribute leftAttr = con.getLhs();

        leftIndex = left.getSchema().indexOf(leftAttr);

        ExternalSort leftRelation = new ExternalSort(left, numBuff, leftIndex, "SRTemp-");

        leftRelation.open();

        return true;
    }



}