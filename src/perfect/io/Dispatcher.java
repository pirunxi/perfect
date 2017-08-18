package perfect.io;

/**
 * Created by HuangQiang on 2017/5/27.
 */
public abstract class Dispatcher {
    public abstract void dispatch(Message msg);


    private static final Dispatcher DEAULT_DISPATCHER = new Dispatcher() {
        @Override
        public void dispatch(Message msg) {
            msg.run();
        }
    };
    public static Dispatcher defaultDispatcher() {
        return DEAULT_DISPATCHER;
    }
}
