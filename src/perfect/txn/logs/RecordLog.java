package perfect.txn.logs;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public abstract class RecordLog {
    public final Object data;
    public RecordLog(Object data) {
        this.data = data;
    }

    public abstract void commit();
}
