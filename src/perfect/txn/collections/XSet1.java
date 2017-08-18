package perfect.txn.collections;

import org.pcollections.Empty;
import org.pcollections.PSet;
import perfect.txn.logs.FieldLog;
import perfect.txn.TKey;
import perfect.txn.Transaction;
import perfect.db.XError;
import perfect.marshal.BinaryStream;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by HuangQiang on 2017/4/24.
 */
public final class XSet1<E> implements XSet<E> {
    private PSet<E> set;
    XSet1(PSet<E> set) {
        this.set = set;
    }
    XSet1() {
        this.set = Empty.set();
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
    public void marshal(BinaryStream os, MarshalValue<E> apply) {
        os.writeCompactUint(set.size());
        for(E e : set) {
            apply.marshal(os, e);
        }
    }

    @Override
    public void unmarshal(BinaryStream os, UnmarshalValue<E> apply) {
        for(int n = os.readCompactUint() ; n > 0 ; n--) {
            set = set.plus(apply.unmarshal(os));
        }
    }

    protected class Log extends FieldLog {
        Log(PSet<E> value) {
            this.value = value;
        }
        PSet<E> value;
        @Override
        public void commit() {
            XSet1.this.set = value;
        }
    }

    @SuppressWarnings("unchecked")
    protected PSet<E> data() {
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        return log != null ? log.value : set;
    }

    @Override
    public Object[] toArray() {
        return data().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return data().toArray(a);
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
    public boolean contains(Object o) {
        return data().contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return data().iterator();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean add(E e) {
        if(e == null) throw new XError("set add null");
        Transaction txn = Transaction.get();
        Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            final PSet<E> oldv = log.value;
            final PSet<E> newv = oldv.plus(e);
            if(oldv != newv) {
                log.value = newv;
                return true;
            } else {
                return false;
            }
        } else {
            final PSet<E> oldv = set;
            final PSet<E> newv = oldv.plus(e);
            if(oldv != newv) {
                txn.putField(this, 0, new Log(newv));
                return true;
            } else {
                return false;
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object o) {
        Transaction txn = Transaction.get();
        Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            final PSet<E> oldv = log.value;
            final PSet<E> newv = oldv.minus(o);
            if(oldv != newv) {
                log.value = newv;
                return true;
            } else {
                return false;
            }
        } else {
            final PSet<E> oldv = set;
            final PSet<E> newv = oldv.minus(o);
            if(oldv != newv) {
                txn.putField(this, 0, new Log(newv));
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return data().containsAll(c);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean addAll(Collection<? extends E> c) {
        Transaction txn = Transaction.get();
        if(c.contains(null)) throw new XError("set addAll null");
        Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            final PSet<E> oldv = log.value;
            final PSet<E> newv = oldv.plusAll(c);
            if(oldv != newv) {
                log.value = newv;
                return true;
            } else {
                return false;
            }
        } else {
            final PSet<E> oldv = set;
            final PSet<E> newv = oldv.plusAll(c);
            if(oldv != newv) {
                txn.putField(this, 0, new Log(newv));
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean removeAll(Collection<?> c) {
        Transaction txn = Transaction.get();
        Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            final PSet<E> oldv = log.value;
            final PSet<E> newv = oldv.minusAll(c);
            if(oldv != newv) {
                log.value = newv;
                return true;
            } else {
                return false;
            }
        } else {
            final PSet<E> oldv = set;
            final PSet<E> newv = oldv.minusAll(c);
            if(oldv != newv) {
                txn.putField(this, 0, new Log(newv));
                return true;
            } else {
                return false;
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
                log.value = Empty.set();
            }
        } else {
            if(!set.isEmpty()) {
                txn.putField(this, 0, new Log(Empty.set()));
            }
        }
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
    public XSet<E> copy() {
        return new XSet1<>(data());
    }

    @Override
    public XSet<E> noTransactionCopy() {
        return new XSet1<>(set);
    }
}
