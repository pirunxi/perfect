package perfect.io;

import perfect.marshal.BinaryStream;

/**
 * Created by HuangQiang on 2017/5/27.
 */
public abstract class Manager<T extends Session> {
    protected abstract void onConnect(Connection conn);
    protected abstract void onClose(Connection conn);

    protected abstract T newSession(Connection conn);
    protected abstract void onAddSession(T session);
    protected abstract void onDelSession(T session);
    public abstract T getSession(int sid);

    protected boolean onUnknownMessage(Connection session, int type, BinaryStream os) {
        return false;
    }
}
