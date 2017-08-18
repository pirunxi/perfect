package perfect.txn.logs;

import perfect.txn.TValue;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public class OriginLog {
    public final TValue value;
    public final Object origin;
    public final long clock;
    public OriginLog(TValue value, Object origin, long clock) {
        this.value = value;
        this.origin = origin;
        this.clock = clock;
    }
}
