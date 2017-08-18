package perfect.txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public abstract class Procedure implements Runnable {
    private final static Logger log = LoggerFactory.getLogger(Procedure.class);
    protected abstract boolean process();


    public final boolean call() {
        final Transaction txn = Transaction.get();
        return txn.inTxn() ? subCall(txn) : topCall(txn);
    }

    @Override
    public final void run() {
        call();
    }

    private final static AtomicLong taskNum = new AtomicLong();
    private final static AtomicLong doNum = new AtomicLong();

    public static long getTaskNum() {
        return taskNum.get();
    }

    public static long getDoNum() {
        return doNum.get();
    }

    private boolean topCall(Transaction txn) {
        taskNum.incrementAndGet();
        txn.begin();
        int count = 0;
        try {
            while(true) {
                try {
                    count++;
                    txn.prepare();
                    if(tasks != null) {
                        txn.addTasks(tasks);
                    }
                    final boolean result = process();
                    if (txn.lockAndCheckNotConflict(result)) {
                        if (result) {
                            txn.commit();
                            log.debug("Procedure:{}. run:{} succ", this.getClass().getName(), count);
                            return true;
                        } else {
                            txn.runRollbackAndFailTaskThenClear();
                            txn.rollback();
                            log.error("Procedure:{}. run:{} fail", this.getClass().getName(), count);
                            return false;
                        }
                    } else {
                        log.debug("Procedure. lockAndCheckNoConflict fail");
                        txn.rollback();
                    }
                } catch (Throwable t) {
                    log.error("Procedure:" + this.getClass().getName() + ". run:" + count, t);
                    t.printStackTrace();
                    if (txn.lockAndCheckNotConflict(false)) {
                        txn.runRollbackAndFailTaskThenClear();
                        txn.rollback();
                        return false;
                    } else {
                        txn.rollback();
                    }
                }
            }
        } finally {
            txn.end();
            doNum.addAndGet(count);
        }
    }

    private boolean subCall(Transaction txn) {
        try {
            txn.prepare();
            if(process()) {
                txn.commit();
                return true;
            } else {
                txn.rollback();
                return false;
            }
        } catch (Throwable t) {
            log.error("Procedure:" + this.getClass().getName() + ". exception:",  t);
            txn.rollback();
            return false;
        }
    }

    public interface Done<P extends Procedure> {
        public void doDone(P p);
    }


    public static <P extends Procedure> void execute(P p, Done<P> done) {
        p.addDoneTask(() -> done.doDone(p));
        p.execute();
    }

    /**
     * 提交到线程池中。
     */
    public void execute() {
        Executor.getNormalExecutor().execute(this);
    }

    private List<Transaction.Task> tasks;
    public Procedure() {
        this.tasks = null;
    }

    public void addRollbackTask(Runnable task) {
        addTask(task, Transaction.TaskType.ON_ROLLBACK);
    }

    public void addSuccTask(Runnable task) {
        addTask(task, Transaction.TaskType.ON_SUCC);
    }

    public void addFailTask(Runnable task) {
        addTask(task, Transaction.TaskType.ON_FAIL);
    }

    public void addDoneTask(Runnable task) {
        addSuccTask(task);
        addFailTask(task);
    }

    private void addTask(Runnable task, Transaction.TaskType type) {
        if(tasks == null) {
            tasks = new ArrayList<>();
        }
        tasks.add(Transaction.Task.create(task, type));
    }
}
