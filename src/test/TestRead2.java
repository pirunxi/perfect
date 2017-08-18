package test;

import perfect.txn.Procedure;

/**
 * Created by HuangQiang on 2016/12/5.
 */
public class TestRead2 extends Procedure {

    private final int n;

    public TestRead2(int n) {
        this.n = n;
    }

    @Override
    protected boolean process() {
        /*
        for(long i = 0 ; i < n ; i++) {
            {
                xbean.Role info = xtable.Roleinfos2.getOrCreate(i);
                info.getA();
                info.getI();
                info.getLi();
                info.getMit();
            }
            {
                xbean.Role info = xtable.Roleinfos2.getOrCreate(i);
                info.getA();
                info.getI();
                info.getLi();
                info.getMit();
            }
        }
        */
        return true;
    }
}
