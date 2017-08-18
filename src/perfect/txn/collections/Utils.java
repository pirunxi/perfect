package perfect.txn.collections;

/**
 * Created by HuangQiang on 2016/12/17.
 */
public class Utils {

    @SuppressWarnings("unchecked")
    public static<E> XList<E> newBeanList(boolean valueTypeIsBean) {
        return valueTypeIsBean ? new XList2() : new XList1<>();
    }

    public static<E> XSet<E> newBeanSet() {
        return new XSet1<>();
    }

    @SuppressWarnings("unchecked")
    public static<K, V> XMap<K, V> newBeanMap(boolean valueTypeIsBean) {
        return valueTypeIsBean ? new XMap2() : new XMap1<>();
    }
}
