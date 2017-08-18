package test;

import perfect.txn.Procedure;

/**
 * Created by HuangQiang on 2016/12/5.
 */
public class TestWrite extends Procedure {

    private final int n;

    public TestWrite(int n) {
        this.n = n;
    }

    @Override
    protected boolean process() {
        /*
        for(int i = 0 ; i < n ; i++) {
            xbean.Role info = xtable.Roleinfos2.getOrCreate((long)i);
            info.setA(i);
            info.setI(i + 1);
            info.getLi();
            info.getMit();
        }
        */
        return true;
    }
}
