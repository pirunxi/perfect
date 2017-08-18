package perfect.txn;

/**
 * Created by HuangQiang on 2017/5/3.
 */
public interface IStorage {
    byte[] find(byte[] key, int ksize);

    void replace(byte[] key, int ksize, byte[] value, int vsize);
    void remove(byte[] key, int ksize);

    interface IWalk {
        boolean onRecord(byte[] key, byte[] data);
    }

    void walk(IWalk walker);
}
