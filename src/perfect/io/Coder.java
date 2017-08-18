package perfect.io;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import org.slf4j.Logger;
import perfect.common.Trace;
import perfect.marshal.BinaryStream;
import perfect.marshal.MarshalException;

import java.util.List;
import java.util.Map;

/**
 * Created by HuangQiang on 2017/5/27.
 */
public class Coder extends ByteToMessageCodec<Object> {

    private final Connection session;
    private final int maxMsgSize;
    private final int sendBuffSize;
    private final int recvBuffSize;
    private final Map<Integer, Message> stubs;

    private final BinaryStream inputBuff;

    private final BinaryStream oneMessageBuff;

    private static final Logger log = Trace.log;

    public Coder(Connection session, Map<Integer, Message> stubs, int maxMsgSize, int sendBuffSize, int recvBuffSize) {
        this.session = session;
        this.stubs = stubs;
        this.maxMsgSize = maxMsgSize;
        this.sendBuffSize = sendBuffSize;
        this.recvBuffSize = recvBuffSize;
        this.inputBuff = new BinaryStream(10240);
        this.oneMessageBuff = new BinaryStream();
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        BinaryStream outputBuff = BinaryStream.getAndClear();
        if(msg instanceof byte[]) {
            byte[] bytes = (byte[])msg;
            outputBuff.writeCompactUint(out, bytes.length);
            out.writeBytes(bytes);
            return;
        }
        try {
            final Message m = (Message)msg;
            Message.encode(m, outputBuff);
            outputBuff.writeTo(out);
        } catch (Exception e) {
            log.error("Coder.encode error.",e);
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        inputBuff.readFrom(in);

        while(inputBuff.nonEmpty()) {
            int mark = inputBuff.readerIndex();
            final int size;
            try {
                size = inputBuff.readCompactUint();
                if(size > maxMsgSize) {
                    ctx.close();
                    log.error("session[{}] decode size:{} exceed maxsize:{}" , session.getSid(), size, maxMsgSize);
                    return;
                }
            } catch (MarshalException e) {
                inputBuff.rollbackReadIndex(mark);
                log.debug("session[{}] read head not enough.", session.getSid());
                break;
            } catch (Exception e) {
                e.printStackTrace();
                ctx.close();
                return;
            }
            if(size > inputBuff.size()) {
                log.debug("session[{}] read body not enough. size:{} remain:{}", session.getSid(), size, inputBuff.size());
                inputBuff.rollbackReadIndex(mark);
                break;
            }
            try {
                oneMessageBuff.wrapRead(inputBuff, size);
                int msgHeadIndex = oneMessageBuff.readerIndex();
                final BinaryStream os = oneMessageBuff;
                int type = os.readCompactUint();
                Message msg = stubs.get(type);
                if(msg == null) {
                    oneMessageBuff.rollbackReadIndex(msgHeadIndex);
                    if(!session.getManager().onUnknownMessage(session, type, os)) {
                        ctx.close();
                        log.debug("session[{}] onUnknownMessage type:{} size:{}", session.getSid(), type, size);
                        return;
                    }
                    continue;
                }
                msg = msg.newObject();
                msg.unmarshal(os);
                out.add(msg);
            } catch (Exception e) {
                e.printStackTrace();
                ctx.close();
                return;
            }
        }

        if(inputBuff.empty())
            inputBuff.clear();
    }
}
