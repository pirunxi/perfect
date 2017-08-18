package perfect.io;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import perfect.common.Trace;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by HuangQiang on 2017/5/27.
 */
public abstract class Server<T extends Session> extends Manager<T> {
    protected final static Logger log = Trace.log;

    private boolean hasOpen;
    private boolean hasClose;
    private final AtomicInteger nextSessionId = new AtomicInteger();
    private final Map<Integer, T> sessions = new ConcurrentHashMap<>();

    private final Conf conf;

    public Server(Conf conf) {
        this.conf = conf;
    }

    public void open() {
        synchronized (this) {
            if(hasOpen) return;
            ServerBootstrap b = new ServerBootstrap();
            b.group(conf.bossGroup, conf.workGroup).channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, conf.backlog)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            new Connection(Server.this, conf.newConnectionConf(), nextSessionId.incrementAndGet(), ch, conf.dispatcher);
                        }
                    }).bind(conf.ip, conf.port);
            log.info("{} bind {}:{}", this.getClass().getName(), conf.ip, conf.port);
        }
    }

    @Override
    public T getSession(int sid) {
        return sessions.get(sid);
    }

    public Conf getConf() {
        return conf;
    }

    @Override
    protected void onConnect(Connection conn) {
        final T session = newSession(conn);
        conn.setContext(session);
        sessions.put(conn.getSid(), session);
        onAddSession(session);
    }

    @Override
    protected void onClose(Connection conn) {
        final T session = sessions.remove(conn.getSid());
        onDelSession(session);
    }

    public boolean send(int sid, Message msg) {
        T c = sessions.get(sid);
        try {
            return c != null && c.send(msg);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean send(int sid, byte[] msg) {
        T c = sessions.get(sid);
        try {
            return c != null && c.send(msg);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean send(int sid, Collection<Message> msgs) {
        T c = sessions.get(sid);
        try {
            return c != null && c.send(msgs);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean send2(int sid, Collection<byte[]> msgs) {
        T c = sessions.get(sid);
        try {
            return c != null && c.send2(msgs);
        } catch (Exception e) {
            return false;
        }
    }

    public static class Conf {
        public String ip = "0.0.0.0";
        public int port;
        public boolean noDelay = false;
        public int backlog = 100;

        public int socketSendBuff = 16 * 1024;
        public int socketRecvBuff = 16 * 1024;
        public int sessionSendBuff = 128 * 1024;
        public int sessionRecvBuff = 128 * 1024;
        public int maxMsgSize = 64 * 1024;

        public NioEventLoopGroup bossGroup;
        public NioEventLoopGroup workGroup;

        public Map<Integer, Message> stubs;
        public Dispatcher dispatcher = Dispatcher.defaultDispatcher();

        public  Connection.Conf newConnectionConf() {
            return new Connection.Conf(noDelay, socketSendBuff, socketRecvBuff, sessionSendBuff, sessionRecvBuff, maxMsgSize, stubs);
        }
    }
}
