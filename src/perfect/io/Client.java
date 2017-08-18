package perfect.io;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.w3c.dom.Element;
import perfect.common.Trace;
import perfect.common.Utils;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by HuangQiang on 2017/5/27.
 */
public abstract class Client<T extends Session> extends Manager<T> {
    protected final static Logger log = Trace.log;

    private boolean hasOpen;
    private boolean hasClose;

    private volatile T session;
    private int nextReconnectInterval;

    private final Conf conf;
    public Client(Conf conf) {
        this.conf = conf;
        this.nextReconnectInterval = conf.initReconnectInterval;
    }

    public Conf getConf() {
        return conf;
    }

    public void open() {
        synchronized (this) {
            if(hasOpen) return;
            hasOpen = true;
            doConnect();
        }
    }

    private void doConnect() {
        log.info("== do connect {}", this.getClass().getName());
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(conf.bossGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, conf.noDelay)
                .option(ChannelOption.SO_SNDBUF, conf.socketSendBuff)
                .option(ChannelOption.SO_RCVBUF, conf.socketRecvBuff)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        new Connection(Client.this, conf.newConnectionConf(), 0, ch, conf.dispatcher);
                    }
                }).connect(conf.ip, conf.port);
    }

    @Override
    protected void onConnect(Connection conn) {
        log.info("== on connect {} sid:{}", this.getClass().getName(), conn.getSid());
        synchronized (this) {
            nextReconnectInterval = conf.initReconnectInterval;
            if(hasClose) {
                conn.close();
            } else {
                if(session != null) {
                    session.close();
                    session = null;
                }
                session = newSession(conn);
                conn.setContext(session);
                onAddSession(session);
            }
        }
    }

    @Override
    protected void onClose(Connection conn) {
        log.info("== onclose {} sid:{}", this.getClass().getName(), conn.getSid());
        synchronized (this) {
            if(session != null && session.getConnection() != conn) {
                return;
            }
            if(session != null) {
                onDelSession(session);
                session = null;
            }

            if(conf.reconnect) {
                conf.bossGroup.schedule(() -> {
                    synchronized (Client.this) {
                        if(hasClose) return;
                        nextReconnectInterval = Math.min(conf.maxReconnectInterval, nextReconnectInterval * 2);
                        doConnect();
                    }
                }, nextReconnectInterval, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    public T getSession(int sid) {
        throw new UnsupportedOperationException();
    }

    public T getSession() {
        return session;
    }

    public boolean send(Message msg) {
        final Session c = session;
        try {
            return c != null && c.send(msg);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean send(Collection<Message> msgs) {
        final Session c = session;
        try {
            return c != null && c.send(msgs);
        } catch (Exception e) {
            return false;
        }
    }

    public void close() {
        synchronized (this) {
            if(!hasOpen || hasClose) return;

            hasClose = true;
            if(session != null)
                session.close();
        }
    }

    public static class Conf {
        public String ip;
        public int port;
        public boolean noDelay = false;
        public int socketSendBuff = 16 * 1024;
        public int socketRecvBuff = 16 * 1024;
        public int sessionSendBuff = 128 * 1024;
        public int sessionRecvBuff = 128 * 1024;
        public int maxMsgSize = 64 * 1024;

        public boolean reconnect = true;
        public int initReconnectInterval = 1;
        public int maxReconnectInterval = 64;


        public void load(Element ele) {
            Utils.readBean(this, ele);
        }

        public NioEventLoopGroup bossGroup;

        public Map<Integer, Message> stubs;
        public Dispatcher dispatcher = Dispatcher.defaultDispatcher();

        public  Connection.Conf newConnectionConf() {
            return new Connection.Conf(noDelay, socketSendBuff, socketRecvBuff, sessionSendBuff, sessionRecvBuff, maxMsgSize, stubs);
        }
    }
}
