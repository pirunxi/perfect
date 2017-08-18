package perfect.txn;

import perfect.marshal.Binary;
import perfect.marshal.BinaryStream;
import perfect.marshal.Marshal;
import perfect.marshal.MarshalException;
import perfect.txn.logs.RootLog;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public interface Bean<T extends Bean> extends Marshal {

    interface IntTransform {
        int apply(int x);
    }

    interface LongTransform {
        long apply(long x);
    }

    interface StringTransform {
        String apply(String x);
    }

    interface BoolTransform {
        boolean apply(boolean x);
    }

    interface ByteTransform {
        byte apply(byte x);
    }

    interface ShortTransform {
        short apply(short x);
    }

    interface FloatTransform {
        float apply(float x);
    }

    interface DoubleTransform {
        double apply(double x);
    }

    interface BinaryTransform {
        Binary apply(Binary x);
    }


    interface MarshalValue<V> {
        void marshal(BinaryStream os, V e);
    }
    interface  UnmarshalValue<V> {
        V unmarshal(BinaryStream os);
    }

    default TKey getRootInTxn(Transaction txn) {
        RootLog log = txn.getRoot(this);
        return log != null ? log.key : null;
    }

    default void setRootInTxn(Transaction txn, TKey newRoot) {
        TKey oldRoot = getRootInTxn(txn);
        if(newRoot != null && oldRoot != null)
            throw OVERRIDE_ROOT_EXCEPTION;
        if(oldRoot == newRoot) return;
        txn.putRoot(this, new RootLog(this, newRoot));
        setChildrenRootInTxn(txn, newRoot);
    }

    default void applyRootInTxn(TKey root) {
        this.setRootDirectly(root);
        applyChildrenRootInTxn(root);
    }

    default int getBeanId() { return 0; }
    TKey getRootDirectly();
    void setRootDirectly(TKey root);
    void setChildrenRootInTxn(Transaction txn, TKey root);
    void applyChildrenRootInTxn(TKey root);
    T copy();
    T noTransactionCopy();

    RuntimeException OVERRIDE_ROOT_EXCEPTION = new RuntimeException("override root exception");

    short TAG_SHIFT = 11;
    short TAG_MASK = (~0) << TAG_SHIFT;
    final class Tag {
        public final static short
                BOOL = 1 << TAG_SHIFT,
                //BYTE = 2 << TAG_SHIFT,
                //SHORT = 3 << TAG_SHIFT,
                INT = 4 << TAG_SHIFT,
                LONG = 5 << TAG_SHIFT,
                FLOAT = 6 << TAG_SHIFT,
                DOUBLE = 7 << TAG_SHIFT,
                BINARY = 8 << TAG_SHIFT,
                STRING = 9 << TAG_SHIFT,
                SET = 10 << TAG_SHIFT,
                LIST = 11 << TAG_SHIFT,
                MAP = 12 << TAG_SHIFT,
                BEAN = 13 << TAG_SHIFT;
    }

    static void skipUnknownField(int tid, BinaryStream _os_) {
        final int tag = (tid & TAG_MASK);
        switch (tag) {
            case Tag.BOOL: _os_.readBool(); break;
            //case Tag.SHORT: _os_.readInt(); break;
            case Tag.INT: _os_.readInt(); break;
            case Tag.LONG: _os_.readLong(); break;
            case Tag.FLOAT: _os_.readFloat(); break;
            case Tag.DOUBLE: _os_.readDouble(); break;
            case Tag.BINARY:
            case Tag.STRING:
            case Tag.LIST:
            case Tag.SET:
            case Tag.MAP:
            case Tag.BEAN:
                _os_.skipBytes(); break;
            default:
                throw new MarshalException("unkown perfect.gendb.tag:" + tag);
        }
    }
}
