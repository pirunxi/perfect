package perfect.io;

import java.util.Collection;

/**
 * Created by HuangQiang on 2017/5/31.
 */
public class Session {
    private final Connection connection;

    public Session(Connection connection) {
        this.connection = connection;
    }

    public int getSessionId() {
        return connection.getSid();
    }

    public Connection getConnection() {
        return connection;
    }

    public void close() {
        connection.close();
    }

    public boolean send(Message msg) {
        return connection.send(msg);
    }

    public boolean send(byte[] msg) {
        return connection.send(msg);
    }

    public boolean send(Collection<Message> msgs) {
        return connection.send(msgs);
    }

    public boolean send2(Collection<byte[]> msgs) {
        return connection.send2(msgs);
    }
}
