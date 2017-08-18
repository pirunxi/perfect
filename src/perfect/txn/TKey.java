package perfect.txn;

import perfect.marshal.Binary;

/**
 * Created by HuangQiang on 2017/4/20.
 */
public abstract class TKey implements Comparable<TKey> {
    public final int tid;

    TKey(int tid) {
        this.tid = tid;
    }


    public static TKey newInt(int tableid, int key) {
        return new TKeyInt(tableid, key);
    }

    public static TKey newLong(int tableid, long key) {
        return new TKeyLong(tableid, key);
    }

    public static TKey newString(int tableid, String key) {
        return new TKeyString(tableid, key);
    }

    public static TKey newBinary(int tableid, Binary key) { return new TKeyBinary(tableid, key); }

    final static class TKeyLong extends TKey {
        public final long key;

        TKeyLong(int tid, long key) {
            super(tid);
            this.key = key;
        }

        @Override
        public int compareTo(TKey o) {
            int c = Long.compare(tid, o.tid);
            if (c != 0) return c;
            return Long.compare(key, ((TKeyLong) o).key);
        }

        @Override
        public final String toString() {
            return String.format("{tid:%s, key:%s}", tid, key);
        }

        @Override
        public final int hashCode() {
            return (tid << 20) ^ Long.hashCode(key);
        }

        @Override
        public final boolean equals(Object o) {
            if(!(o instanceof TKeyLong)) return false;
            TKeyLong b = (TKeyLong) o;
            return tid == b.tid && key == b.key;
        }
    }


    final static class TKeyInt extends TKey {
        public final int key;

        TKeyInt(int tid, int key) {
            super(tid);
            this.key = key;
        }

        @Override
        public int compareTo(TKey o) {
            int c = Integer.compare(tid, o.tid);
            if (c != 0) return c;
            return Integer.compare(key, ((TKeyInt) o).key);
        }

        @Override
        public final String toString() {
            return String.format("{tid:%s, key:%s}", tid, key);
        }

        @Override
        public final int hashCode() {
            return (tid << 20) ^ key;
        }

        @Override
        public final boolean equals(Object o) {
            if(!(o instanceof TKeyInt)) return false;
            TKeyInt b = (TKeyInt) o;
            return tid == b.tid && key == b.key;
        }
    }


    final static class TKeyString extends TKey {
        public final String key;

        TKeyString(int tid, String key) {
            super(tid);
            this.key = key;
        }

        @Override
        public int compareTo(TKey o) {
            int c = Long.compare(tid, o.tid);
            if (c != 0) return c;
            return key.compareTo(((TKeyString) o).key);
        }

        @Override
        public final String toString() {
            return String.format("{tid:%s, key:%s}", tid, key);
        }

        @Override
        public final int hashCode() {
            return (tid << 20) ^ key.hashCode();
        }

        @Override
        public final boolean equals(Object o) {
            if(!(o instanceof TKeyString)) return false;
            TKeyString b = (TKeyString) o;
            return tid == b.tid && key.equals(b.key);
        }
    }

    final static class TKeyBinary extends TKey {
        public final Binary key;

        TKeyBinary(int tid, Binary key) {
            super(tid);
            this.key = key;
        }

        @Override
        public int compareTo(TKey o) {
            int c = Long.compare(tid, o.tid);
            if (c != 0) return c;
            return key.compareTo(((TKeyBinary) o).key);
        }

        @Override
        public final String toString() {
            return String.format("{tid:%s, key:%s}", tid, key);
        }

        @Override
        public final int hashCode() {
            return (tid << 20) ^ key.hashCode();
        }

        @Override
        public final boolean equals(Object o) {
            if(!(o instanceof TKeyBinary)) return false;
            TKeyBinary b = (TKeyBinary) o;
            return tid == b.tid && key.equals(b.key);
        }
    }
}

