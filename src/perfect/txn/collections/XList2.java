package perfect.txn.collections;

import org.pcollections.Empty;
import org.pcollections.PVector;
import org.pcollections.TreePVector;
import perfect.txn.TKey;
import perfect.txn.Transaction;
import perfect.txn.Bean;
import perfect.db.XError;

import java.util.Collection;

/**
 * Created by HuangQiang on 2017/4/24.
 */
public final class XList2<E extends Bean<E>> extends XList1<E> {
    private XList2(PVector<E> list) {
        super(list);
    }

    XList2() {
        super();
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
        e.setRootInTxn(txn, getRootInTxn(txn));
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
                ((E)o).setRootInTxn(txn, null);
                return true;
            } else {
                return false;
            }
        } else {
            PVector<E> oldv = list;
            PVector<E> newv = oldv.minus(o);
            if(oldv != newv) {
                txn.putField(this, 0, new Log(newv));
                ((E)o).setRootInTxn(txn, null);
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
        if(c.contains(null)) throw new XError("list add null");
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            log.value = log.value.plusAll(c);
        } else {
            txn.putField(this, 0, new Log(list.plusAll(c)));
        }
        final TKey root = getRootInTxn(txn);
        for(E e : c) {
            e.setRootInTxn(txn, root);
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        if(c.isEmpty()) return false;
        if(c.contains(null)) throw new XError("list add null");
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            log.value = log.value.plusAll(index, c);
        } else {
            txn.putField(this, 0, new Log(list.plusAll(index, c)));
        }
        final TKey root = getRootInTxn(txn);
        for(E e : c) {
            e.setRootInTxn(txn, root);
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean removeAll(Collection<?> c) {
        boolean change = false;
        for(Object e : c) {
            if(remove(e)) {
                change = true;
            }
        }
        return change;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void clear() {
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        if(log != null) {
            if(!log.value.isEmpty()) {
                for(E e : log.value)
                    e.setRootInTxn(txn, null);
                log.value = TreePVector.empty();
            }
        } else {
            if(!list.isEmpty()) {
                for(E e : list)
                    e.setRootInTxn(txn, null);
                txn.putField(this, 0, new Log(TreePVector.empty()));
            }
        }
    }


    @SuppressWarnings("unchecked")
    @Override
    public E set(int index, E element) {
        if(element == null) throw new XError("list set null");
        Transaction txn = Transaction.get();
        final Log log = (Log)txn.getField(this, 0);
        element.setRootInTxn(txn, getRootInTxn(txn));
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
        olde.setRootInTxn(txn, null);
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
        element.setRootInTxn(txn, getRootInTxn(txn));
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
            if(oldv != newv) {
                log.value = newv;
            }
        } else {
            PVector<E> oldv = list;
            olde = oldv.get(index);
            PVector<E> newv = oldv.minus(index);
            if(oldv != newv) {
                txn.putField(this, 0, new Log(newv));
            }
        }
        olde.setRootInTxn(txn, null);
        return olde;
    }

    @Override
    public void setChildrenRootInTxn(Transaction txn, TKey root) {
        for(E e : data()) {
            e.setRootInTxn(txn, root);
        }
    }

    @Override
    public void applyChildrenRootInTxn(TKey root) {
        for(E e : list) {
            e.applyRootInTxn(root);
        }
    }

    @Override
    public XList<E> copy() {
        final PVector<E> oldv = data();
        PVector<E> newv = Empty.vector();
        for(E e : oldv) {
            newv = newv.plus(e.copy());
        }
        return new XList2<>(newv);
    }

    @Override
    public XList<E> noTransactionCopy() {
        final PVector<E> oldv = list;
        PVector<E> newv = Empty.vector();
        for(E e : oldv) {
            newv = newv.plus(e.noTransactionCopy());
        }
        return new XList2<>(newv);
    }
}
