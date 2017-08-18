package perfect.txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import perfect.common.Tuple2;
import perfect.txn.logs.FieldLog;
import perfect.txn.logs.OriginLog;
import perfect.txn.logs.RecordLog;
import perfect.txn.logs.RootLog;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public final class Transaction {
    private final static Logger log = LoggerFactory.getLogger(Transaction.class);


    private static final AtomicLong CLOCK = new AtomicLong();
    static long nextClock() {
        return CLOCK.incrementAndGet();
    }

    private final static ThreadLocal<Transaction> threadLocalTxns = ThreadLocal.withInitial(Transaction::new);
    public static Transaction get() {
        return threadLocalTxns.get();
    }

    private final static int MAX_TRANSACTION_DEPTH = 10;
    private final TreeMap<TKey, OriginLog> origins = new TreeMap<>();
    private final SavePoint[] savepoints = new SavePoint[MAX_TRANSACTION_DEPTH];
    private final List<Tuple2<TKey, Lock>> locks = new ArrayList<>();
    private final HashSet<TKey> writes = new HashSet<>();
    private int depth = 0;

    private TreeMap<TKey, RecordLog> datas = new TreeMap<>();
    private HashMap<CKey, FieldLog> fields = new HashMap<>();
    private HashMap<Bean<?>, RootLog> roots = new HashMap<>();
    private List<Task> tasks = new ArrayList<>();

    OriginLog getOrigin(TKey key) {
        return origins.get(key);
    }

    void putOrigin(TKey key, OriginLog origin) {
        assert (inTxn());
        origins.put(key, origin);
    }

    RecordLog getData(TKey key) {
        assert (inTxn());
        final RecordLog data = datas.get(key);
        if(data != null) return data;
        for(int i = depth - 2 ; i >= 0 ; i--) {
            final SavePoint cp = savepoints[i];
            RecordLog d = cp.datas.get(key);
            if(d != null)
                return d;
        }
        return null;
    }

    void putData(TKey key, RecordLog data) {
        if(!inTxn()) throw new Error("putData out of transaction");
        datas.put(key, data);
    }

    private CKey cacheKey = new CKey(null, 0);
    public FieldLog getField(Bean obj, int index) {
        final CKey key = cacheKey.replace(obj, index);
        FieldLog log = fields.get(key);
        if(log != null) return log;
        for(int i = depth - 2 ; i >= 0 ; i--) {
            final SavePoint cp = savepoints[i];
            FieldLog f = cp.fields.get(key);
            if(f != null)
                return f;
        }
        return null;
    }

    public void putField(Bean obj, int index, FieldLog log) {
        if(!inTxn()) throw new Error("putField out of transaction");
        fields.put(new CKey(obj, index), log);
    }

    RootLog getRoot(Bean data) {
        RootLog log = roots.get(data);
        if(log != null) return log;
        for(int i = depth - 2 ; i >= 0 ; i--) {
            final SavePoint cp = savepoints[i];
            RootLog r = cp.roots.get(data);
            if(r != null)
                return r;
        }
        return null;
    }

    void putRoot(Bean data, RootLog log) {
        if(!inTxn()) throw new Error("putRoot out of transaction");
        roots.put(data, log);
    }

    final boolean inTxn() {
        return depth > 0;
    }

    void begin() {
        assert (depth == 0);
        assert (locks.isEmpty());
    }

    void end() {
        assert (depth == 0);
        assert (writes.isEmpty());
        assert (datas.isEmpty());
        assert (fields.isEmpty());
        assert (roots.isEmpty());

        for(Tuple2<TKey, Lock> p : locks) {
            p._2.unlock();
        }
        locks.clear();
        this.tasks.clear();
    }

    void prepare() {
        if(depth == 0) {
            assert (origins.isEmpty());
            assert (datas.isEmpty());
            assert (fields.isEmpty());
            assert (roots.isEmpty());

            datas.clear();
            fields.clear();
            roots.clear();
        } else {
            assert (savepoints[depth - 1] == null);
            savepoints[depth - 1] = new SavePoint(datas, fields, roots, tasks);
            datas = new TreeMap<>();
            fields = new HashMap<>();
            roots = new HashMap<>();
            tasks = new ArrayList<>();
        }
        depth++;
    }

    void commit() {
        assert (depth > 0);
        if(depth == 1) {
            for(RecordLog d : datas.values()) {
                d.commit();
            }
            datas.clear();

            for(FieldLog f : fields.values()) {
                f.commit();
            }
            fields.clear();

            // root 的commit放到最后,这样容器类 applyRootInTxn时
            // 可以不必从log里取数据了
            for(RootLog r : roots.values()) {
                r.commit();
            }
            roots.clear();

            final long newVersion = nextClock();
            for(TKey key : writes) {
                if(key != null) {
                    TValue tvalue = origins.get(key).value;
                    tvalue.setDirty(true);
                    tvalue.setVersion(newVersion);
                }
            }
            writes.clear();
            origins.clear();
            runSuccTasks(this.tasks);
            this.tasks.clear();
        } else {
            final SavePoint cp = savepoints[depth - 2];
            savepoints[depth - 2] = null;
            cp.datas.putAll(datas);
            cp.fields.putAll(fields);
            cp.roots.putAll(roots);
            datas = cp.datas;
            fields = cp.fields;
            roots = cp.roots;

            for(Task task : tasks) {
                if(task.type != TaskType.ON_ROLLBACK) {
                    cp.tasks.add(task);
                }
            }
            tasks = cp.tasks;
        }
        depth--;
    }

    void rollback() {
        assert (depth > 0);
        if(depth == 1) {
            datas.clear();
            fields.clear();
            roots.clear();
            writes.clear();
            origins.clear();
            runRollbackTasks(this.tasks);
        } else {
            final SavePoint cp = savepoints[depth - 2];
            savepoints[depth - 2] = null;
            datas = cp.datas;
            fields = cp.fields;
            roots = cp.roots;
            runRollbackAndFailTasks(this.tasks);
            tasks = cp.tasks;
        }
        depth--;
    }

    boolean lockAndCheckNotConflict(boolean succ) {
        assert (writes.isEmpty());
        boolean noConflict = true;
        if(succ) {
            for(Map.Entry<TKey, RecordLog> e : datas.entrySet()) {
                if(origins.get(e.getKey()).origin != e.getValue().data)
                    writes.add(e.getKey());
            }
            for(CKey k : fields.keySet()) {
                TKey tk = k.getObj().getRootDirectly();
                if(tk != null)
                    writes.add(tk);
            }
        }
        int lockIndex = 0;
        final long now = System.currentTimeMillis();
        for(Map.Entry<TKey, OriginLog> e : origins.entrySet()) {
            final TKey key = e.getKey();
            final OriginLog log = e.getValue();
            final boolean isWrite = writes.contains(key);
            final Lock lock = isWrite ? log.value.getRwlock().writeLock() : log.value.getRwlock().readLock();
            final int len = locks.size();
            log.value.setAccessTime(now);

            boolean find = false;
            for(; lockIndex < len ; lockIndex++) {
                final Tuple2<TKey, Lock> p = locks.get(lockIndex);
                int c = p._1.compareTo(key);
                if(c == 0 && p._2 == lock) {
                    find = true;
                    ++lockIndex;
                    break;
                } else if(c >= 0) {
                    // unlock locks since lockIndex
                    for(int i = lockIndex ; i < len ; i++)
                        locks.get(i)._2.unlock();
                    locks.subList(lockIndex, len).clear();
                    break;
                }
                // else next.
            }
            // 如果没找到.lockIndex已经是正确的lockIndex了
            if(!find) {
                lock.lock();
                locks.add(new Tuple2<>(key, lock));
            }
            noConflict = noConflict && log.clock == log.value.getVersion();
        }
        return noConflict;
    }

    private void addTask(Runnable task, TaskType type) {
        this.tasks.add(Task.create(task, type));
    }

    private void addTask(String uniqueKey, Runnable task, TaskType type) {
        for(Task t : this.tasks) {
            if(t.uniqueKey != null && t.uniqueKey.equals(uniqueKey)) {
                t.replace(task, type);
                return;
            }
        }
        this.tasks.add(Task.create(uniqueKey, task, type));
    }

    public void addRollbackTask(Runnable task) {
        addTask(task, TaskType.ON_ROLLBACK);
    }

    private void addSuccTask(Runnable task) {
        addTask(task, TaskType.ON_SUCC);
    }

    private void addSuccTask(String uniqueKey, Runnable task) {
        addTask(uniqueKey, task, TaskType.ON_SUCC);
    }

    private void addFailTask(Runnable task) {
        addTask(task, TaskType.ON_FAIL);
    }

    public void addDoneTask(Runnable task) {
        addSuccTask(task);
        addFailTask(task);
    }

    void addTasks(List<Task> tasks) {
        this.tasks.addAll(tasks);
    }

    enum TaskType {
        ON_ROLLBACK,
        ON_FAIL,
        ON_SUCC
    }

    final static class Task {
        TaskType type;
        Runnable task;
        final String uniqueKey;
        private Task(Runnable task, TaskType type, String uniqueKey) {
            this.task = task;
            this.type = type;
            this.uniqueKey = uniqueKey;
        }

        void replace(Runnable task, TaskType type) {
            this.task = task;
            this.type = type;
        }

        static Task create(Runnable task, TaskType type) {
            return new Task(task, type, null);
        }

        static Task create(String uniqueKey, Runnable task, TaskType type) {
            return new Task(task, type, uniqueKey);
        }
    }

    private static void runSuccTasks(List<Task> tasks) {
        for(Task task : tasks) {
            if(task.type == TaskType.ON_SUCC) {
                try {
                    task.task.run();
                } catch (Throwable e) {
                    log.error("Transaction2 run task:" + task.task, e);
                }
            }
        }
    }
    private static void runRollbackTasks(List<Task> tasks) {
        for(Task task : tasks) {
            if(task.type == TaskType.ON_ROLLBACK) {
                try {
                    task.task.run();
                } catch (Throwable e) {
                    log.error("Transaction2 run task:" + task.task, e);
                }
            }
        }
    }

    void runRollbackAndFailTaskThenClear() {
        for(Task task : tasks) {
            if(task.type == TaskType.ON_ROLLBACK || task.type == TaskType.ON_FAIL) {
                try {
                    task.task.run();
                } catch (Throwable e) {
                    log.error("Transaction2 run task:" + task.task, e);
                }
            }
        }
        tasks.clear();
    }

    private static void runRollbackAndFailTasks(List<Task> tasks) {
        for(Task task : tasks) {
            if(task.type == TaskType.ON_ROLLBACK || task.type == TaskType.ON_FAIL) {
                try {
                    task.task.run();
                } catch (Throwable e) {
                    log.error("Transaction2 run task:" + task.task, e);
                }
            }
        }
    }

    public static void asyncExecuteWhileCommit(Runnable task) {
        get().addSuccTask(new ExecuteRunnable(task));
    }
    public static void syncExecuteWhileCommit(Runnable task) {
        get().addSuccTask(task);
    }

    public static void asyncExecuteWhileCommit(Procedure proc) {
        get().addSuccTask(new ExecuteProcedure(proc));
    }

    @Deprecated
    public static void syncExecuteWhileCommit(Procedure proc) throws Exception {
        throw new RuntimeException("can' sync execute procedure while commit");
    }

    public static void syncExecuteWhileCommit(String uniqueKey, Runnable task) {
        get().addSuccTask(uniqueKey, task);
    }


    public static class ExecuteProcedure implements Runnable {
        private final Procedure proc;

        ExecuteProcedure(Procedure proc) {
            this.proc = proc;
        }

        @Override
        public void run() {
            this.proc.execute();
        }
    }

    public static class ExecuteRunnable implements Runnable {
        private final Runnable command;

        ExecuteRunnable(Runnable r) {
            this.command = r;
        }

        @Override
        public void run() {
            Executor.getNormalExecutor().execute(command);
        }
    }

    public static final class SavePoint {
        final TreeMap<TKey, RecordLog> datas;
        final HashMap<CKey, FieldLog> fields;
        final HashMap<Bean<?>, RootLog> roots;
        final List<Task> tasks;
        SavePoint(TreeMap<TKey, RecordLog> datas, HashMap<CKey, FieldLog> fields, HashMap<Bean<?>, RootLog> roots, List<Task> tasks) {
            this.datas = datas;
            this.fields = fields;
            this.roots = roots;
            this.tasks = tasks;
        }
    }

    public static final class CKey {
        private Bean<?> obj;
        private int index;
        CKey(Bean<?> obj, int index) {
            this.obj = obj;
            this.index = index;
        }

        Bean<?> getObj() {
            return obj;
        }

        public int getIndex() {
            return index;
        }

        @Override
        public int hashCode() {
            return (index << 20) ^ obj.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            final CKey b = (CKey)o;
            return obj == b.obj && index == b.index;
        }

        CKey replace(Bean<?> obj, int index) {
            this.obj = obj;
            this.index = index;
            return this;
        }
    }
}
