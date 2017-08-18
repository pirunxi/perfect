package perfect.db;

import org.slf4j.*;
import org.slf4j.Logger;

/**
 * Created by HuangQiang on 2017/4/21.
 */
public class Checkpoint extends Thread {
    private final static Logger log = LoggerFactory.getLogger(Checkpoint.class);

    enum State {
        NORMAL,
        CHECKPOINT_NOW,
        CLOSE,
    }

    private State state = State.NORMAL;

    @Override
    public void run() {
        Database database = Database.getIns();
        final long checkpointInterval = 10 * 1000L;
//        final long shrinkInterval = xconf.getShrinkPeriod() * 1000L;
        long nextCheckpointTime = System.currentTimeMillis() + checkpointInterval;
//        long nextShrinkTime = System.currentTimeMillis() + shrinkInterval;
        while(true) {
            if(System.currentTimeMillis() >= nextCheckpointTime) {
                database.checkpoint();
                database.shrink();
                nextCheckpointTime = System.currentTimeMillis() + checkpointInterval;
            }
//            if(System.currentTimeMillis() >= nextShrinkTime) {
//                nextShrinkTime = System.currentTimeMillis() + shrinkInterval;
//            }
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                log.warn("checkpoint.sleep interrupted");
                database.checkpoint();
                database.shrink();
                break;
            }
        }
    }

    public void close() {
        synchronized (this) {
            state = State.CLOSE;
        }
        this.interrupt();
    }
}
