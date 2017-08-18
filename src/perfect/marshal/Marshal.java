package perfect.marshal;

/**
 * Created by HuangQiang on 2017/5/2.
 */
public interface Marshal {
    void marshal(BinaryStream bs);
    void unmarshal(BinaryStream bs);
}
