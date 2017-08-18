package test;

/**
 * Created by HuangQiang on 2017/4/22.
 */
public class TestRun {
    /*
    public static class TestReadWrite extends perfect.txn.Procedure {
        private final List<Long> writes;
        private final List<Long> reads;
        private final boolean succ;
        public TestReadWrite(List<Long> writes, List<Long> reads, boolean succ) {
            this.writes = writes;
            this.reads = reads;
            this.succ = succ;
        }

        @Override
        protected boolean process() {
            for(long roleid : writes) {
                xbean.Role role = xtable.Role1.createIfNotExist(roleid);
                role.i(x -> x + 1);
                role._2(x -> !x);
                role.d(d -> d + 1.2);
                role.l(x -> x + 11);
                //Trace.info("roleid1:{} role:{}", roleid, role);
            }
            for(long roleid : reads) {
                xbean.Role role = xtable.Role1.createIfNotExist(roleid);
                role.i();
                //Trace.info("roleid2:{} role:{}", roleid, role);
            }
            return succ;
        }
    }

    public static List<Long> randomRoleids(int N, int M, Random random) {
        final List<Long> roleids = new ArrayList<>();
        for (int n = 1 + random.nextInt(M); n-- > 0; ) {
            roleids.add((long)random.nextInt(N));
        }
        return roleids;
    }

    public static void main(String[] argv) {
        PropertyConfigurator.configure("log4j.properties");
        XdbConf xconf = new XdbConf("perfect.gendb.config.xml");
        Xdb.getNormalExecutor().init(xconf);
        try {
            Xdb.getNormalExecutor().start();
            final int THREAD_NUM = 4;
            final int N = 100000;
            {
                for(int j = 0 ; j < THREAD_NUM ; j++) {
                    final int seed = j;
                    new Thread(() -> {
                        final Random random = new Random(seed);
                        while(true) {
                            TestReadWrite p1 = new TestReadWrite(randomRoleids(N, 5, random), randomRoleids(N, 10, random), true);
                            p1.call();

                            new Procedure() {
                                @Override
                                protected boolean process() {
                                    return true;
                                }
                            }.call();

                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }).start();
                }
            }
            long lastTime = System.currentTimeMillis();
            long lastTaskNum = 0;
            long lastDoNum = 0;
            while(true) {
                Thread.sleep(1000);
                final long taskNum = Procedure.getTaskNum();
                final long doNum =Procedure.getDoNum();
                final long deltaTaskNum = taskNum - lastTaskNum;
                final long deltaDoNum = doNum - lastDoNum;

                final long now = System.currentTimeMillis();
                final long elpaseTime = now - lastTime;
                lastTime = now;
                lastTaskNum = taskNum;
                lastDoNum = doNum;
                Trace.info("== taskNum:{} doNum:{} redoRate:{} aver:{}", taskNum, doNum, (double)(deltaDoNum - deltaTaskNum) * 100 / deltaTaskNum, deltaTaskNum * 1000 / elpaseTime);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Xdb.getNormalExecutor().stop();
        }
        System.exit(1);
    }
    */
}
