package test;


import perfect.txn.Procedure;

/**
 * Created by HuangQiang on 2016/12/5.
 */
public class Test3 extends Procedure {
    @Override
    protected boolean process() {
        return false;
    }
    /*

    private final Map<Long, Boolean> keys = new LinkedHashMap<>();
    public Test3(List<Long> readKeys, List<Long> writeKeys) {
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
            if(info == null) {
                info = xbean.Pod.newRole();
                xtable.Roleinfos2.insert(k, info);
            }
            if(modify) {
                info.getI();
                info.getLi();
                info.getMit();

                info.setI(info.getI() + 1);
                info.setA(info.getA() + 1);
                info.getT().setI(info.getT().getI() + 1);
            } else {
                info.getA();
                info.getI();
                info.getT().getI();
            }
        });

        return true;
    }

    private static List<Long> randomKeys(Random random, int n) {
        final ArrayList<Long> keys = new ArrayList<>();
        for(int i = 0 ; i < n ; i++)
            keys.add((long)random.nextInt(40000));
        return keys;
    }

    public static void main(String[] argv) throws InterruptedException {


        for(int k = 0 ; k < 4 ; k++) {
            final int seed = k;
            new Thread() {
                @Override
                public void run() {
                    final Random random = new Random(seed);
                    for(;;)
                        new Test3(randomKeys(random, 30), randomKeys(random, 10)).call();
                }
            }.start();
        }

        long lastTaskNum = 0;
        long lastRetryNum = 0;
        for(;;) {
            Thread.sleep(1000);
            final long retryNum = Procedure2.retryNum.get();
            final long taskNum = Procedure2.taskNum.get();
            System.out.printf("conflict %s/%s/%s = rate:%.3f%% %n", taskNum - lastTaskNum, retryNum - lastRetryNum, taskNum, 100.0 * (retryNum - lastRetryNum) / (taskNum - lastTaskNum));
            lastTaskNum = taskNum;
            lastRetryNum = retryNum;
        }
    }
    */
}
