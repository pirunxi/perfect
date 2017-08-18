package perfect.txn;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * Created by HuangQiang on 2016/12/4.
 */
public final class TValue {
    private volatile Object data;
    private volatile long accessTime;
    private volatile long version;
    private volatile boolean dirty;
    private final ReadWriteLock rwlock;

    public TValue(Object data, long accessTime, long version, ReadWriteLock rwlock) {
        this.data = data;
        this.accessTime = accessTime;
        this.version = version;
        this.rwlock = rwlock;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public long getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public ReadWriteLock getRwlock() {
        return rwlock;
    }
}
