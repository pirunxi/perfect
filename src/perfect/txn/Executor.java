package perfect.txn;

import java.util.concurrent.*;

/**
 * Created by HuangQiang on 2017/5/3.
 */
public class Executor {

    private static ExecutorService normalExcutor;
    private static ScheduledExecutorService scheduleExecutor;
    public static ExecutorService getNormalExecutor() {
        return normalExcutor;
    }

    public static ScheduledExecutorService getScheduleExecutor() {
        return scheduleExecutor;
    }

    public static synchronized void start(int coreThreadNum, int maxThreadNum, int coreScheduleThreadNum) {
        if(normalExcutor != null)
            throw new Error("Executor has started.");
        normalExcutor = new ThreadPoolExecutor(coreThreadNum, maxThreadNum,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());
        scheduleExecutor = Executors.newScheduledThreadPool(coreScheduleThreadNum);
    }

    public static synchronized void stop() {
        if(normalExcutor != null) {
            normalExcutor.shutdown();
            normalExcutor = null;
        }
        if(scheduleExecutor != null) {
            scheduleExecutor.shutdown();
            scheduleExecutor = null;
        }
    }
}
