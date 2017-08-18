package perfect.txn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import perfect.marshal.BinaryStream;
import perfect.txn.logs.OriginLog;
import perfect.txn.logs.RecordLog;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public abstract class Table<K, V> {
    private final static Logger log = LoggerFactory.getLogger(Table.class);

    public static final class Conf {
        public final IStorage storage;
        public final int maxCacheRecordNum;
        public final int shrinkPeriod;
        public final int shrinkRecordExpireTime;

        public Conf(IStorage storage, int maxCacheRecordNum, int shrinkPeriod, int shrinkRecordExpireTime) {
            this.storage = storage;
            this.maxCacheRecordNum = maxCacheRecordNum;
            this.shrinkPeriod = shrinkPeriod;
            this.shrinkRecordExpireTime = shrinkRecordExpireTime;
        }
    }

    private static int NEXT_TABLEID = 0;

    private final String name;
    private final int id;
    private final boolean persistent;
    private final boolean valueTypeIsBean;

    private Conf conf;
    private IStorage storage;
    private final ConcurrentHashMap<K, TValue> datas = new ConcurrentHashMap<>();

    public Table(String name, boolean persistent, boolean valueTypeIsBean) {
        this.name = name;
        this.persistent = persistent;
        this.id = ++NEXT_TABLEID;
        this.storage = null;
        this.valueTypeIsBean = valueTypeIsBean;
    }

    public abstract void marshalKey(BinaryStream os, K key);
    public abstract K unmarshalKey(BinaryStream os);
    public abstract void marshalValue(BinaryStream os, V value);
    public abstract V unmarshalValue(BinaryStream os);
    public abstract TKey makeTKey(K key);

    @Override
    public String toString() {
        return String.format("table{name:%s, datas:%d}", getName(), datas.size());
    }
    public final int getId() {
        return id;
    }

    public final String getName() {
        return name;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public void open(Conf conf) {
        this.conf = conf;
        this.storage = conf.storage;
    }

    public void close() {
        if(storage != null) {
            storage = null;
        }
    }

    @SuppressWarnings("unchecked")
    private V load(K key) {
        if(storage != null) {
            final BinaryStream okey = new BinaryStream();
            marshalKey(okey, key);
            byte[] bvalue = storage.find(okey.array(), okey.size());
            if (bvalue != null) {
                try {
                    return unmarshalValue(BinaryStream.wrap(bvalue));
                } catch (Exception e) {
                    throw new Error(String.format("table:%s key:%s value:%s unmarshal fail", name, key, bvalue), e);
                }
            }
        }
        return null;
    }

    public interface IWalker<KK, VV> {
        boolean walk(KK k, VV v);
    }

    @SuppressWarnings("unchecked")
    public void walk(IWalker<K, V> walker) {
        if(storage != null) {
            storage.walk((okey, ovalue) -> {
                try {
                    final K key = unmarshalKey(BinaryStream.wrap(okey));
                    final V value = unmarshalValue(BinaryStream.wrap(ovalue));
                    return walker.walk(key, value);
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            });
        } else {
            for(Map.Entry<K, TValue> e : datas.entrySet()) {
                final TValue value = e.getValue();
                final Lock lock = value.getRwlock().readLock();
                lock.lock();
                try {
                    if (!walker.walk(e.getKey(), (V) value.getData()))
                        return;
                } catch (Exception ee) {
                    ee.printStackTrace();
                    return;
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private TValue gett(K key, TKey tkey) {
        TValue tvalue = datas.get(key);
        if (tvalue != null)
            return tvalue;
        final V value = load(key);
        synchronized (datas) {
            tvalue = datas.get(key);
            if(tvalue != null)
                return tvalue;

            if(value != null && (value instanceof Bean)) {
                ((Bean)value).applyRootInTxn(tkey);
            }
            tvalue = new TValue(value, System.currentTimeMillis(), Transaction.nextClock(), new ReentrantReadWriteLock());
            datas.put(key, tvalue);
            return  tvalue;
        }
    }

    @SuppressWarnings("unchecked")
    public V get(K key) {
        final Transaction txn = Transaction.get();
        if(!txn.inTxn()) throw new Error("table.get out of transaction");
        final TKey tkey = makeTKey(key);
        final RecordLog log = txn.getData(tkey);
        if(log != null) return (V)log.data;
        while (true) {
            final TValue tvalue = gett(key, tkey);
            // 注意，以下两行的顺序非常重要,调换顺序可能导致极端情况下出现错误的结果
            final long version = tvalue.getVersion();
            // 这个数据正巧被shrink掉了,重取一次
            if (version < 0) continue;
            final Object origin = tvalue.getData();
            txn.putOrigin(tkey, new OriginLog(tvalue, origin, version));
            return (V)origin;
        }
    }

    @SuppressWarnings("unchecked")
    public V add(K key, V value) {
        if (value == null) throw new NullPointerException();
        final Transaction txn = Transaction.get();
        if(!txn.inTxn()) throw new Error("table.add out of transaction");

        final TKey tkey = makeTKey(key);
        if(valueTypeIsBean) {
            ((Bean) value).setRootInTxn(txn, tkey);
        }

        final RecordLog log = txn.getData(tkey);
        txn.putData(tkey, new RecordLog(value) {
            @Override
            public void commit() {
                datas.get(key).setData(this.data);
            }
        });
        final Object oldValue;
        if(log != null) {
            oldValue = log.data;
        } else {
            final OriginLog originLog = txn.getOrigin(tkey);
            if(originLog != null) {
                oldValue = originLog.origin;
            } else {
                final TValue tvalue = gett(key, tkey);
                final long version = tvalue.getVersion();
                oldValue = tvalue.getData();
                txn.putOrigin(tkey, new OriginLog(tvalue, oldValue, version));
            }
        }
        if(oldValue != null && valueTypeIsBean) {
            ((Bean)oldValue).setRootInTxn(txn, null);
        }
        return (V)oldValue;
    }

    public V put(K key, V value) {
        return add(key, value);
    }

    public void insert(K key, V value) {
        if(add(key, value) != null)
            throw new Error(String.format("key:%s exist", key));
    }

    @SuppressWarnings("unchecked")
     private V remove(K key) {
        final Transaction txn = Transaction.get();
        if(!txn.inTxn()) throw new Error("table.remove out of transaction");
        final TKey tkey = makeTKey(key);
        final RecordLog tlog = txn.getData(tkey);
        final Object oldValue;
        txn.putData(tkey, new RecordLog(null) {
            @Override
            public void commit() {
                datas.get(key).setData(null);
            }
        });
        if(tlog != null) {
            oldValue = tlog.data;
        } else {
            OriginLog olog = txn.getOrigin(tkey);
            if(olog != null) {
                oldValue = olog.origin;
            } else {
                final TValue tvalue = gett(key, tkey);
                final long version = tvalue.getVersion();
                oldValue = tvalue.getData();
                txn.putOrigin(tkey, new OriginLog(tvalue, oldValue, version));
            }
        }
        if(oldValue != null && valueTypeIsBean) {
            ((Bean)oldValue).setRootInTxn(txn, null);
        }
        return (V)oldValue;
    }

    public void delete(K key) {
        if(remove(key) != null)
            throw new Error(String.format("key:%s not exist", key));
    }

    @SuppressWarnings("unchecked")
    public <T> T select(K key, Function<V, T> func) {
        final Transaction txn = Transaction.get();
        if(txn.inTxn()) {
            final V value = get(key);
            return value != null ? func.apply(value) : null;
        } else {
            final TValue tvalue = gett(key, makeTKey(key));
            final V value = (V)tvalue.getData();
            if(value == null) return null;
            final Lock lock = tvalue.getRwlock().readLock();
            lock.lock();
            try {
                return func.apply(value);
            } finally {
                lock.unlock();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public V select(K key) {
        final Transaction txn = Transaction.get();
        final TKey tkey = makeTKey(key);
        if(txn.inTxn()) {
            final V value = get(key);
            return value != null ? (valueTypeIsBean ? (V)((Bean)value).copy() : value) : null;
        } else {
            final TValue tvalue = datas.get(key);
            if(tvalue != null) {
                final V value = (V)tvalue.getData();
                if(value == null) return null;
                final Lock lock = tvalue.getRwlock().readLock();
                lock.lock();
                try {
                    return valueTypeIsBean ? (V)((Bean)value).noTransactionCopy() : value;
                } finally {
                    lock.unlock();
                }
            } else {
                return null;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void flushDirties() {
        if(storage == null) return;
        long beginTime = System.currentTimeMillis();
        final BinaryStream okey = new BinaryStream();
        final BinaryStream ovalue = new BinaryStream();

        int count = 0;
        int totalSize = 0;
        for(Map.Entry<K, TValue> e : datas.entrySet()) {
            final K key = e.getKey();
            final TValue tvalue = e.getValue();

            if(tvalue.isDirty()) {
                okey.clear();
                ovalue.clear();

                final Lock lock = tvalue.getRwlock().readLock();
                lock.lock();
                final Object data;
                try {
                    ++count;
                    data = tvalue.getData();
                    if(data != null) {
                        marshalValue(ovalue, (V)data);
                    }
                    tvalue.setDirty(false);
                } finally {
                    lock.unlock();
                }
                marshalKey(okey, key);
                totalSize += okey.size();
                if(data != null) {
                    storage.replace(okey.array(), okey.size(), ovalue.array(), ovalue.size());
                    totalSize += ovalue.size();
                    log.debug(" flush dirty. replace key:{} value:{}", key, data);
                } else {
                    storage.remove(okey.array(), okey.size());
                    log.debug(" flush dirty. remove  key:{}", key);
                }
            }
        }
        long costTime = System.currentTimeMillis() - beginTime;
        if(costTime > 300)
            log.info("[flush] table:{} count:{} totalsize:{} cost time:{} ms", getName(), count, totalSize, costTime);
    }

    public void shrink() {
        if(datas.size() <= conf.maxCacheRecordNum) return;
        long now = System.currentTimeMillis();
        long minExpireInterval = conf.shrinkPeriod * 1000L;
        long curExpireTime = Math.min(conf.shrinkRecordExpireTime * 1000L, now - datas.values().stream().mapToLong(TValue::getAccessTime).min().getAsLong() - minExpireInterval);

        log.info("[shrink] begin. table:{} size:{} maxCacheRecordSize:{}", getName(), datas.size(), conf.maxCacheRecordNum);
        while(datas.size() > conf.maxCacheRecordNum && curExpireTime > minExpireInterval) {
            for (Iterator<Map.Entry<K, TValue>> it = datas.entrySet().iterator(); it.hasNext(); ) {
                final Map.Entry<K, TValue> e = it.next();
                final TValue tvalue = e.getValue();
                if (!tvalue.isDirty() && tvalue.getAccessTime() + curExpireTime < now) {
                    final Lock lock = tvalue.getRwlock().writeLock();
                    lock.lock();
                    try {
                        if (!tvalue.isDirty()) {
                            tvalue.setVersion(-1);
                            it.remove();
                            log.debug("== shrink table:{} key:{} value:{}", getName(), e.getKey(), tvalue.getData());
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
            curExpireTime >>= 1;
        }
        log.info("[shrink] end. table:{} size:{} maxCacheRecordSize:{}", getName(), datas.size(), conf.maxCacheRecordNum);
    }

}
