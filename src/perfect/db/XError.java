package perfect.db;

/**
 * Created by HuangQiang on 2016/12/8.
 */
public class XError extends RuntimeException {
    public XError(String message) {
        super(message);
    }

    public XError(String message, Throwable e) {
        super(message, e);
    }
}
