package test;

import perfect.txn.Procedure;

/**
 * Created by HuangQiang on 2016/12/5.
 */
public class Test2 extends Procedure {
/*
    public static void print(xbean.Role info) {
        System.out.println("i=" + info.getI()+ ",_1=" + info.getA() +",t.i=" + info.getT().getI() + ",si=" + info.getSi() + ",t.si=" + info.getT().getSi()
                + ",li=" + info.getLi() + ",t.li=" + info.getT().getLi() +",mii=" + info.getMii() +",mit=" + info.getMit() + ",t.mii=" + info.getT().getMii()
        +",t.mit=" + info.getT().getMit());
    }

    private final Map<Long, Boolean> keys = new LinkedHashMap<>();
    public Test2(List<Long> readKeys, List<Long> writeKeys) {
        for(long k : readKeys) {
            keys.put(k, false);
        }
        for(long k : writeKeys) {
            keys.put(k, true);
        }
    }

    @Override
    protected boolean process() {
        //System.out.println("====" + keys.keySet());
        keys.forEach((k, modify) -> {
            xbean.Role info = xtable.Roleinfos2.get(k);
            if(modify) {
                info.setA(info.getA() + 1);
                info.setI(info.getI() + 10);
                info.getT().setI(info.getT().getI() + 100);
            } else {
                info.getA();
                info.getI();
                info.getT().getI();
            }
        });

        return true;
    }

    private final static Random random = new Random(1218);
    private static List<Long> randomKeys(int n) {
        final ArrayList<Long> keys = new ArrayList<>();
        for(int i = 0 ; i < n ; i++)
            keys.add((long)random.nextInt(10000));
        return keys;
    }

    public static void main(String[] argv) throws InterruptedException {
        Executor executor = Executors.newFixedThreadPool(32);

        long N = 10000;
        long M = 10;
        long total = N * M;
        for(int j = 0 ; j < M ; j++) {
            for (int i = 0; i < N; i++) {
                executor.execute(() -> new Test2(randomKeys(random.nextInt(20) + 1), randomKeys(random.nextInt(10))).call());
            }
            Thread.sleep(500);
        }
            Thread.sleep(2000);
            System.out.printf("conflict rate:%.3f%% %n" , (Procedure2.retryNum.get() - total) * 100.0 / total);
        for(long i = 0 ; i < 100 ; i++)
            print(xtable.Roleinfos2.getNotTxnReadOnly(i));
        System.exit(0);

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
