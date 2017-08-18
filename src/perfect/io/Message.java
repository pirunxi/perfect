package perfect.io;

import perfect.marshal.BinaryStream;

import java.util.Map;

/**
 * Created by HuangQiang on 2017/5/2.
 */
public abstract class Message implements Bean, Runnable {

    public abstract Message newObject();

    private Object ctx;
    public void setContext(Object ctx) {
        this.ctx = ctx;
    }

    public Object getContext() {
        return ctx;
    }

    public static void encode(Message m, BinaryStream bs) {
        bs.writeCompactUint(m.getTypeId());
        m.marshal(bs);
    }

    public static Message decode(Map<Integer, Message> stubs, BinaryStream bs) {
        int type = bs.readCompactUint();
        Message m = stubs.get(type);
        if(m != null) {
            m.unmarshal(bs);
            return m;
        } else {
            return null;
        }
    }

    private final static ThreadLocal<BinaryStream> localBinaryStreams = ThreadLocal.withInitial(() -> new BinaryStream(10240));
    public byte[] encodeToBytes() {
        BinaryStream bs = localBinaryStreams.get();
        bs.clear();
        encode(this, bs);
        return bs.remainCopy();
    }
}
