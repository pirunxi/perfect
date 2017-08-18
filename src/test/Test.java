package test;

import perfect.txn.Procedure;

/**
 * Created by HuangQiang on 2016/12/5.
 */
public class Test extends Procedure {
/*
    public static void print(xbean.Role info) {
        System.out.println("i=" + info.getI()+ ",_1=" + info.getA() +",t.i=" + info.getT().getI() + ",si=" + info.getSi() + ",t.si=" + info.getT().getSi()
                + ",li=" + info.getLi() + ",t.li=" + info.getT().getLi() +",mii=" + info.getMii() +",mit=" + info.getMit() + ",t.mii=" + info.getT().getMii()
        +",t.mit=" + info.getT().getMit() + ",lt=" + info.getLt());
    }

    @Override
    protected boolean process() {
        if(!xtable.Roleinfos.exists(0L)) {
            System.out.println("### not exist. create!");
            xbean.Role ni = xbean.Pod.newRole();
            ni.setI(12);
            ni.setA(18);
            ni.getT().setI(120);
            for(int i = 0 ; i < 5 ; i++) {
                ni.getSi().add(i);
                ni.getT().getSi().add(i * 10);
                ni.getLi().add(i);
                ni.getT().getLi().add(i * 10);

                ni.getMii().put(i, 1000 + i);
                ni.getT().getMii().put(i, 1000 + i);
                xbean.User2 u = new xbean.rw.User2();
                u.setI(i);
                u.getT().setI(-i);
                ni.getLt().add(u);
            }
            ni.getSi().remove(0);
            ni.getLi().remove((Integer)0);
            ni.getLi().remove(1);
            ni.getMii().remove(1);
            ni.getT().getSi().remove(0);
            ni.getT().getLi().remove((Integer)0);
            ni.getT().getLi().remove(1);
            ni.getT().getMii().remove(1);

            ni.getLt().remove(0);
            xtable.Roleinfos2.put(0L, ni);
            print(ni);
        }

        xbean.Role info2 = xtable.Roleinfos2.get(0L);
        info2.getSi().remove(3);
        info2.getT().getSi().remove(20);
        info2.getLi().remove((Integer)4);
        info2.getT().getLi().remove((Integer)10);
        info2.getSi().add(10);
        info2.getT().getSi().add(100);
        info2.getLi().add(10);
        info2.getT().getLi().add(100);
        info2.getMii().put(10, 1218);
        info2.getT().getMii().put(10, 1218);
        info2.getLt().remove(0);
        xbean.User2 u = new xbean.rw.User2();
        u.setI(1218);
        info2.getLt().add(u);
        u = info2.getLt().get(0);
        u.setI(u.getI() + 100);
        print(info2);
        return true;
    }


    public static void main(String[] argv) {
        Database.init();
        for(int i = 0 ; i < 2 ; i++) {
            new Test().call();

            xbean.Role info = xtable.Roleinfos2.getNotTxnReadOnly(0L);
            print(info);
            Database.checkpoint();
            System.out.println("==========");
        }


        Database.checkpoint();
        Database.close();
        //info.setI(1);
        //info.getSi().remove(4);
        //info.getLi().remove(1);
        //info.getLi().remove((Integer)1);
        //info.getT().getLi().remove(1);
    }
    */

    @Override
    protected boolean process() {
        return false;
    }
}
