package test;

import perfect.txn.Procedure;

/**
 * Created by HuangQiang on 2016/12/5.
 */
public class TestReadWrite2 extends Procedure {

    private final int n;

    public TestReadWrite2(int n) {
        this.n = n;
    }

    @Override
    protected boolean process() {
        /*
        for(int i = 0 ; i < n ; i++) {
            {
                xbean.Role info = xtable.Roleinfos2.getOrCreate((long)i);
                info.setI(i);
                info.getI();
                info.getLi();
                info.getMit();
            }
            {
                xbean.Role info = xtable.Roleinfos2.getOrCreate((long)i);
                info.setI(i);
                info.getI();
                info.getLi();
                info.getMit();
            }
        }
        */
        return true;
    }
}
