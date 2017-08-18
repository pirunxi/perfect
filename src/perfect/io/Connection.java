package perfect.io;

import io.netty.channel.*;
import org.slf4j.Logger;
import perfect.common.Trace;

import java.util.Collection;
import java.util.Map;

/**
 * Created by HuangQiang on 2017/5/27.
 */
public class Connection extends ChannelDuplexHandler {
    private final static Logger log = Trace.log;
    private final Manager manager;
    private final Conf conf;
    private final int sid;
    private final Channel channel;
    private final Dispatcher dispatcher;

    private volatile boolean hasClosed;
    private Object ctx;
    public Connection(Manager manager, Conf conf, int sid, Channel channel, Dispatcher dispatcher) {
        this.manager = manager;
        this.conf = conf;
        this.sid = sid;
        this.channel = channel;
        this.dispatcher = dispatcher;

        ChannelConfig cc = channel.config();
        cc.setOption(ChannelOption.TCP_NODELAY, conf.noDelay);
        cc.setOption(ChannelOption.SO_SNDBUF, conf.socketSendBuff);
        cc.setOption(ChannelOption.SO_RCVBUF, conf.socketRecvBuff);

        channel.pipeline().addLast("coder", new Coder(this, conf.stubs, conf.maxMsgSize, conf.sessionSendBuff, conf.sessionRecvBuff)).addLast("dispatcher", this);
    }

    @Override
    public String toString() {
        return "Connection:" + sid;
    }

    public Manager getManager() {
        return manager;
    }

    public int getSid() {
        return sid;
    }

    public Object getContext() {
        return ctx;
    }

    public void setContext(Object ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        log.debug("== handlerRemove");
        synchronized (this) {
            this.hasClosed = true;
        }
        manager.onClose(this);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.debug("== channel active");
        manager.onConnect(this);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Connection sid:" + sid, cause);
    }

//    @Override
//    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//        System.out.println("== channel inactive");
//    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        onRecv((Message)msg);
    }

    private void onRecv(Message msg) {
        msg.setContext(ctx);
        log.debug("== recv:{}", msg);
        dispatcher.dispatch(msg);
    }

    public boolean send(Message msg) {
        if(hasClosed) return false;
        try {
            log.debug("== send:{}", msg);
            channel.writeAndFlush(msg);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean send(byte[] msg) {
        if(hasClosed) return false;
        try {
            log.debug("== send:{}", msg);
            channel.writeAndFlush(msg);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean send(Collection<Message> msgs) {
        if(hasClosed) return false;
        try {
            for(Message msg : msgs)
                channel.write(msg);
            channel.flush();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean send2(Collection<byte[]> msgs) {
        if(hasClosed) return false;
        try {
            for(byte[] msg : msgs)
                channel.write(msg);
            channel.flush();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void close() {
        synchronized (this) {
            if(hasClosed) return;
            channel.close();
        }
    }

    public static class Conf {
        public final boolean noDelay;
        public final int socketSendBuff;
        public final int socketRecvBuff;
        public final int sessionSendBuff;
        public final int sessionRecvBuff;
        public final int maxMsgSize;
        public Map<Integer, Message> stubs;

        public Conf(boolean noDelay, int socketSendBuff, int socketRecvBuff, int sessionSendBuff, int sessionRecvBuff, int maxMsgSize, Map<Integer, Message> stubs) {
            this.noDelay = noDelay;
            this.socketSendBuff = socketSendBuff;
            this.socketRecvBuff = socketRecvBuff;
            this.sessionSendBuff = sessionSendBuff;
            this.sessionRecvBuff = sessionRecvBuff;
            this.maxMsgSize = maxMsgSize;
            this.stubs = stubs;
        }
    }
}
