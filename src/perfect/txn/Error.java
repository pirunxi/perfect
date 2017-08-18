package perfect.txn;

/**
 * Created by HuangQiang on 2017/5/3.
 */
public class Error extends RuntimeException {
    public Error(String message) {
        super(message);
    }

    public Error(String message, Throwable cause) {
        super(message, cause);
    }

    public Error(Throwable cause) {
        super(cause);
    }
}
