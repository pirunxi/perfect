package perfect.txn.collections;

import perfect.txn.Bean;
import perfect.marshal.BinaryStream;

import java.util.List;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public interface XList<E> extends Bean<XList<E>>, List<E> {
    void marshal(BinaryStream os, MarshalValue<E> apply);
    void unmarshal(BinaryStream os, UnmarshalValue<E> apply);
}
