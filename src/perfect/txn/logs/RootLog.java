package perfect.txn.logs;

import perfect.txn.Bean;
import perfect.txn.TKey;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public class RootLog {
    public final Bean<?> data;
    public final TKey key;

    public RootLog(Bean<?> data, TKey key) {
        this.data = data;
        this.key = key;
    }

    public void commit() {
        this.data.setRootDirectly(key);
    }
}
