package perfect.txn.collections;

import org.pcollections.Empty;
import org.pcollections.PVector;
import org.pcollections.TreePVector;
import perfect.txn.logs.FieldLog;
import perfect.txn.TKey;
import perfect.txn.Transaction;
import perfect.db.XError;
import perfect.marshal.BinaryStream;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by HuangQiang on 2017/4/24.
 */
public class XList1<E> implements XList<E> {

    protected PVector<E> list;
    protected XList1(PVector<E> list) {
        this.list = list;
    }

    XList1() {
        this.list = Empty.vector();
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
        os.writeCompactUint(list.size());
        for(E e : list) {
            apply.marshal(os, e);
        }
    }

    @Override
    public void unmarshal(BinaryStream os, UnmarshalValue<E> apply) {
        for(int n = os.readCompactUint() ; n > 0 ; n--) {
            list = list.plus(apply.unmarshal(os));
        }
    }

    protected class Log extends FieldLog {
        Log(PVector<E> value) {
            this.value = value;
        }
        PVector<E> value;
        @Override
        public void commit() {
            XList1.this.list = value;
        }
    }

    @SuppressWarnings("unchecked")
    protected PVector<E> data() {
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        return log != null ? log.value : list;
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
    public boolean containsAll(Collection<?> c) {
        return data().containsAll(c);
    }

    @Override
    public ListIterator<E> listIterator() {
        return data().listIterator();
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        return data().listIterator(index);
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        return data().subList(fromIndex, toIndex);
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
        if(e == null) throw new XError("list add null");
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            log.value = log.value.plus(e);
        } else {
            txn.putField(this, 0, new Log(list.plus(e)));
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object o) {
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            PVector<E> oldv = log.value;
            PVector<E> newv = oldv.minus(o);
            if(oldv != newv) {
                log.value = newv;
                return true;
            } else {
                return false;
            }
        } else {
            PVector<E> oldv = list;
            PVector<E> newv = oldv.minus(o);
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
    public boolean addAll(Collection<? extends E> c) {
        if(c.isEmpty()) return false;
        if(c.contains(null)) throw new XError("list addAll null");
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            log.value = log.value.plusAll(c);
        } else {
            txn.putField(this, 0, new Log(list.plusAll(c)));
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        if(c.isEmpty()) return false;
        if(c.contains(null)) throw new XError("list addAll null");
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            log.value = log.value.plusAll(index, c);
        } else {
            txn.putField(this, 0, new Log(list.plusAll(index, c)));
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean removeAll(Collection<?> c) {
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            PVector<E> oldv = log.value;
            PVector<E> newv = oldv.minusAll(c);
            if(oldv != newv) {
                log.value = newv;
                return true;
            } else {
                return false;
            }
        } else {
            PVector<E> oldv = list;
            PVector<E> newv = oldv.minusAll(c);
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
    public void clear() {
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            if(!log.value.isEmpty()) {
                log.value = TreePVector.empty();
            }
        } else {
            if(!list.isEmpty()) {
                txn.putField(this, 0, new Log(TreePVector.empty()));
            }
        }
    }

    @Override
    public E get(int index) {
        return data().get(index);
    }

    @SuppressWarnings("unchecked")
    @Override
    public E set(int index, E element) {
        if(element == null) throw new XError("list set null");
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        final E olde;
        if(log != null) {
            PVector<E> oldv = log.value;
            olde = oldv.get(index);
            PVector<E> newv = oldv.with(index, element);
            if(oldv != newv) {
                log.value = newv;
            }
        } else {
            PVector<E> oldv = list;
            olde = oldv.get(index);
            PVector<E> newv = oldv.with(index, element);
            if(oldv != newv) {
                txn.putField(this, 0, new Log(newv));
            }
        }
        return olde;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(int index, E element) {
        if(element == null) throw new XError("list add null");
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            log.value = log.value.plus(index, element);
        } else {
            txn.putField(this, 0, new Log(list.plus(index, element)));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public E remove(int index) {
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        final E olde;
        if(log != null) {
            PVector<E> oldv = log.value;
            olde = oldv.get(index);
            PVector<E> newv = oldv.minus(index);
            log.value = newv;
        } else {
            PVector<E> oldv = list;
            olde = oldv.get(index);
            PVector<E> newv = oldv.minus(index);
            txn.putField(this, 0, new Log(newv));
        }
        return olde;
    }

    @Override
    public int indexOf(Object o) {
        return data().indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return data().lastIndexOf(o);
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
    public XList<E> copy() {
        return new XList1<>(data());
    }

    @Override
    public XList<E> noTransactionCopy() {
        return new XList1<>(list);
    }
}
