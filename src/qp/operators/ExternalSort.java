package qp.operators;

import qp.utils.Attribute;

public class ExternalSort extends Operator {

    private Operator table;
    private Attribute attribute;
    private int numbuff;

    public ExternalSort(Operator table, Attribute joinAttr, int numBuff) {
        super(OpType.SORT);
        this.table = table;
        this.attribute = joinAttr;
        this.numbuff = numBuff;
    }

    public boolean open() {
        return true;
    }
}
