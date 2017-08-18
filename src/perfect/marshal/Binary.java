package perfect.marshal;


/**
 * Created by HuangQiang on 2017/5/2.
 */
public final class Binary implements Comparable<Binary> {
    private final byte[] bytes;
    private Binary(byte[] bytes) {
        this.bytes = bytes;
    }

    private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private final static Binary EMPTY = new Binary(EMPTY_BYTE_ARRAY);
    public static Binary empty() {
        return EMPTY;
    }
    public static byte[] emptyByteArray() {
        return EMPTY_BYTE_ARRAY;
    }

    public static Binary copy(byte[] bytes) {
        return new Binary(bytes.clone());
    }

    public static Binary wrap(byte[] bytes) {
        return new Binary(bytes);
    }

    /**
     *
     * @param s  x.y.z._1._2.c
     * @return
     */
    public static Binary fromDotString(String s) {
        final String[] cells = s.split("\\.");
        final int n = cells.length;
        final byte[] bs = new byte[n];
        for(int i = 0 ; i < n ; i++)
            bs[i] = Byte.parseByte(cells[i]);
        return new Binary(bs);
    }

    @Override
    public int compareTo(Binary o) {
        int c = bytes.length - o.bytes.length;
        if(c != 0) return c;
        for(int i = 0, n = bytes.length ; i < n ; i++)
            if((c = Byte.compare(bytes[i], o.bytes[i])) != 0)
                return c;
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof  Binary)) return false;
        byte[] bs = ((Binary)o).bytes;
        if(bs.length != bytes.length) return false;
        for(int i = 0, n = bytes.length ; i < n ; i++)
            if(bytes[i] != bs[i])
                return false;
        return true;
    }

    public String toString() {
        return "#" + bytes.length;
    }

    public byte[] array() {
        return bytes;
    }

    public boolean isEmpty() {
        return bytes.length == 0;
    }
}
