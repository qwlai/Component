package qp.operators;

import qp.utils.Attribute;

public class SortMergeJoin extends Join {
    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getJoinType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        Attribute leftattr = con.getLhs();

        ExternalSort leftRelation = new ExternalSort(left, leftattr, numBuff);
        return true;
    }
}
