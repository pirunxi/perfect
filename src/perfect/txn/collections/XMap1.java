package perfect.txn.collections;

import org.pcollections.Empty;
import org.pcollections.PMap;
import perfect.txn.logs.FieldLog;
import perfect.txn.TKey;
import perfect.txn.Transaction;
import perfect.db.XError;
import perfect.marshal.BinaryStream;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by HuangQiang on 2017/4/24.
 */
public class XMap1<K, V> implements XMap<K, V> {
    protected PMap<K, V> map;
    protected XMap1(PMap<K, V> map) {
        this.map = map;
    }
    XMap1() {
        this.map = Empty.map();
    }

    @Override
    public void marshal(BinaryStream os) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unmarshal(BinaryStream os) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void marshal(BinaryStream os, MarshalValue<K> key, MarshalValue<V> value) {
        os.writeCompactUint(map.size());
        for(Map.Entry<K, V> e : map.entrySet()) {
            key.marshal(os, e.getKey());
            value.marshal(os, e.getValue());
        }
    }

    @Override
    public void unmarshal(BinaryStream os, UnmarshalValue<K> key, UnmarshalValue<V> value) {
        for(int n = os.readCompactUint(); n > 0 ; n--) {
            map = map.plus(key.unmarshal(os), value.unmarshal(os));
        }
    }

    protected class Log extends FieldLog {
        Log(PMap<K, V> value) {
            this.value = value;
        }
        PMap<K, V> value;
        @Override
        public void commit() {
            XMap1.this.map = value;
        }
    }

    @SuppressWarnings("unchecked")
    protected PMap<K, V> data() {
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        return log != null ? log.value : map;
    }

    @Override
    public boolean containsValue(Object value) {
        return data().containsValue(value);
    }

    @Override
    public int size() {
        return data().size();
    }

    @Override
    public boolean isEmpty() {
        return data().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return data().containsKey(key);
    }

    @Override
    public V get(Object key) {
        return data().get(key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public V put(K key, V value) {
        if(value == null) throw new XError("map put null");
        Transaction txn = Transaction.get();
        Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            final PMap<K, V> oldm = log.value;
            log.value = oldm.plus(key, value);
            return oldm.get(key);
        } else {
            final PMap<K, V> oldm = map;
            final PMap<K, V> newm = oldm.plus(key, value);
            txn.putField(this, 0, new Log(newm));
            return oldm.get(key);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key) {
        Transaction txn = Transaction.get();
        Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            final PMap<K, V> oldm = log.value;
            final PMap<K, V> newm = oldm.minus(key);
            if(oldm != newm) {
                log.value = newm;
            }
            return oldm.get(key);
        } else {
            final PMap<K, V> oldm = map;
            final PMap<K, V> newm = oldm.minus(key);
            if(oldm != newm) {
                txn.putField(this, 0, new Log(newm));
            }
            return oldm.get(key);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Transaction txn = Transaction.get();
        //if(m.values().contains(null)) throw new XError("map putAll null");
        Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            final PMap<K, V> oldm = log.value;
            final PMap<K, V> newm = oldm.plusAll(m);
            if(oldm != newm) {
                log.value = newm;
            }
        } else {
            final PMap<K, V> oldm = map;
            final PMap<K, V> newm = oldm.plusAll(m);
            if(oldm != newm) {
                txn.putField(this, 0, new Log(newm));
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void clear() {
        Transaction txn = Transaction.get();
        Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            if(!log.value.isEmpty()) {
                log.value = Empty.map();
            }
        } else {
            if(!map.isEmpty()) {
                txn.putField(this, 0, new Log(Empty.map()));
            }
        }
    }

    @Override
    public Set<K> keySet() {
        return data().keySet();
    }

    @Override
    public Collection<V> values() {
        return data().values();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return data().entrySet();
    }

    private TKey _root_;
    @Override
    public TKey getRootDirectly() {
        return _root_;
    }

    @Override
    public void setRootDirectly(TKey root) {
        _root_ = root;
    }

    @Override
    public void setChildrenRootInTxn(Transaction txn, TKey root) {

    }

    @Override
    public void applyChildrenRootInTxn(TKey root) {

    }

    @Override
    public XMap<K, V> copy() {
        return new XMap1<>(data());
    }

    @Override
    public XMap<K, V> noTransactionCopy() {
        return new XMap1<>(map);
    }
}
