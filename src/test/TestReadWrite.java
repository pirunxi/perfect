package test;

import perfect.txn.Procedure;

/**
 * Created by HuangQiang on 2016/12/5.
 */
public class TestReadWrite extends Procedure {

    private final int n;

    public TestReadWrite(int n) {
        this.n = n;
    }

    @Override
    protected boolean process() {
        /*
        for(long i = 0 ; i < n ; i++) {
            xbean.Role info = xtable.Roleinfos2.getOrCreate(i);
            info.setI((int)i);
            info.getI();
            info.getLi();
            info.getMit();
        }
        */
        return true;
    }
}
