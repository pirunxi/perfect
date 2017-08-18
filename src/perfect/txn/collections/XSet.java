package perfect.txn.collections;


import perfect.marshal.BinaryStream;
import perfect.txn.Bean;

import java.util.Set;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public interface XSet<E> extends Bean< XSet<E> >, Set<E> {
    void marshal(BinaryStream os, MarshalValue<E> apply);
    void unmarshal(BinaryStream os, UnmarshalValue<E> apply);
}
