package perfect.txn.collections;

import org.pcollections.Empty;
import org.pcollections.PMap;
import perfect.txn.TKey;
import perfect.txn.Transaction;
import perfect.txn.Bean;
import perfect.db.XError;

import java.util.Map;

/**
 * Created by HuangQiang on 2017/4/24.
 */
public class XMap2<K, V extends Bean<V>> extends XMap1<K, V> {
    protected XMap2(PMap<K, V> map) {
        super(map);
    }

    XMap2() {
        super();
    }

    @SuppressWarnings("unchecked")
    @Override
    public V put(K key, V value) {
        if(value == null) throw new XError("talbe.put value == null");
        Transaction txn = Transaction.get();
        TKey root = getRootInTxn(txn);
        value.setRootInTxn(txn, root);
        Log log = (Log)txn.getField(this, 0);
        final V oldv;
        if(log != null) {
            final PMap<K, V> oldm = log.value;
            log.value = oldm.plus(key, value);
            oldv = oldm.get(key);
        } else {
            final PMap<K, V> oldm = map;
            txn.putField(this, 0, new Log(oldm.plus(key, value)));
            oldv = oldm.get(key);
        }
        if(oldv != null)
            oldv.setRootInTxn(txn, null);
        return oldv;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key) {
        Transaction txn = Transaction.get();
        Log log = (Log)txn.getField(this, 0);
        final V oldv;
        if(log != null) {
            final PMap<K, V> oldm = log.value;
            final PMap<K, V> newm = oldm.minus(key);
            if(oldm != newm) {
                log.value = newm;
            }
            oldv = oldm.get(key);
        } else {
            final PMap<K, V> oldm = map;
            final PMap<K, V> newm = oldm.minus(key);
            if(oldm != newm) {
                txn.putField(this, 0, new Log(newm));
            }
            oldv = oldm.get(key);
        }
        if(oldv != null) {
            oldv.setRootInTxn(txn, null);
        }
        return oldv;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Transaction txn = Transaction.get();
        TKey root = getRootInTxn(txn);
        Log log = (Log)txn.getField(this, 0);
        final PMap<K, V> oldm = log != null ? log.value : map;
        PMap<K, V> newm = oldm;
        for(Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            final K key = e.getKey();
            final V value = e.getValue();
            value.setRootInTxn(txn, root);
            newm = newm.plus(key, value);
            final V oldv = oldm.get(key);
            if(oldv != null)
                oldv.setRootInTxn(txn, null);
        }
        if(oldm != newm) {
            if(log != null) {
                log.value = newm;
            } else {
                txn.putField(this, 0, new Log(newm));
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void clear() {
        Transaction txn = Transaction.get();
        TKey root = getRootInTxn(txn);
        Log log = (Log)txn.getField(this, 0);
        final PMap<K, V> oldm = log != null ? log.value : map;
        if(oldm.isEmpty()) return;
        for(V v : oldm.values()) {
            v.setRootInTxn(txn, null);
        }

        if(log != null) {
            log.value = Empty.map();
        } else {
            txn.putField(this, 0, new Log(Empty.map()));
        }
    }

    @Override
    public void setChildrenRootInTxn(Transaction txn, TKey root) {
        for(V v : values()) {
            v.setRootInTxn(txn, root);
        }
    }

    @Override
    public void applyChildrenRootInTxn(TKey root) {
        for(V v : map.values()) {
            v.applyRootInTxn(root);
        }
    }

    @Override
    public XMap<K, V> copy() {
        final PMap<K, V> oldm = data();
        PMap<K, V> newm = Empty.map();
        for(Map.Entry<K, V> e : oldm.entrySet()) {
            newm = newm.plus(e.getKey(), e.getValue().copy());
        }
        return new XMap2<>(newm);
    }

    @Override
    public XMap<K, V> noTransactionCopy() {
        final PMap<K, V> oldm = data();
        PMap<K, V> newm = Empty.map();
        for(Map.Entry<K, V> e : oldm.entrySet()) {
            newm = newm.plus(e.getKey(), e.getValue().noTransactionCopy());
        }
        return new XMap2<>(newm);
    }
}
