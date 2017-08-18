package perfect.marshal;

/**
 * Created by HuangQiang on 2017/5/2.
 */
public class MarshalException extends RuntimeException {
    public MarshalException(String message) {
        super(message);
    }

    public MarshalException(String message, Throwable cause) {
        super(message, cause);
    }

    public MarshalException(Throwable cause) {
        super(cause);
    }

    public MarshalException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
