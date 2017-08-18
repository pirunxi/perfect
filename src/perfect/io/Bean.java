package perfect.io;

import perfect.marshal.Marshal;

/**
 * Created by HuangQiang on 2017/5/27.
 */
public interface Bean extends Marshal {
    int getTypeId();
}
