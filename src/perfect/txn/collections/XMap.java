package perfect.txn.collections;

import perfect.txn.Bean;
import perfect.marshal.BinaryStream;

import java.util.Map;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public interface XMap<K, V> extends Bean<XMap<K, V>>, Map<K, V> {
    void marshal(BinaryStream os, MarshalValue<K> key, MarshalValue<V> value);
    void unmarshal(BinaryStream os, UnmarshalValue<K> key, UnmarshalValue<V> value);
}
