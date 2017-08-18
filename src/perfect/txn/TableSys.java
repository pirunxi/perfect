package perfect.txn;

import org.slf4j.*;
import perfect.db.XError;
import perfect.marshal.BinaryStream;

import java.util.HashMap;

public final class TableSys extends Table<Long, TableSys.TableSysData> {
    private final static org.slf4j.Logger log = LoggerFactory.getLogger(TableSys.class);

	private final static TableSys table = new TableSys("_sys_");
	public static TableSys getTable() {
		return table;
	}

	public TableSys(String name) {
		super(name, true, true);
	}

	@Override
	public void marshalKey(BinaryStream os, Long key) {
		os.writeLong(key);
	}

	@Override
	public Long unmarshalKey(BinaryStream os) {
		return os.readLong();
	}

    @Override
    public void marshalValue(BinaryStream os, TableSysData value) {
        TableSysData.marshalBean(value, os);
    }

    @Override
    public TableSysData unmarshalValue(BinaryStream os) {
	    return TableSysData.unmarshalBean(os);
    }

    @Override
	public TKey makeTKey(Long key) {
		return TKey.newLong(getId(), key);
	}

	public TableSysData createIfNotExist(Long key) {
	    TableSysData value = get(key);
	    if(value == null) {
	        value = TableSysData.newBean();
	        add(key, value);
        }
	    return value;
    }

	private boolean inited;
	private long key;
	private boolean dirty;
	private final HashMap<String, Long> nextids = new HashMap<>();
	public void init(int localid) {
	    this.key = localid;
	    if(!new Procedure() {
            @Override
            protected boolean process() {
                final TableSysData data = table.createIfNotExist(key);
                nextids.putAll(data.nextids());
                return true;
            }
        }.call())
	        throw new XError("tablesys init fail.");
	    inited = true;
	    log.info("tablesys.init nextids:{}", nextids);
    }

    public long nextid(String groupName, long initValue, long step) {
	    synchronized (this) {
	        dirty = true;
	        final long curid = nextids.getOrDefault(groupName, initValue);
	        final long newid = curid + step;
	        nextids.put(groupName, newid);
	        return curid;
        }
    }

    @Override
    public void flushDirties() {
	    final HashMap<String, Long> newNextids;
	    synchronized (this) {
            if (!inited || !dirty) return;
            dirty = false;
            newNextids = new HashMap<>(nextids);
        }
        if(!new Procedure() {
            @Override
            protected boolean process() {
                final TableSysData data = table.createIfNotExist(key);
                data.nextids().putAll(newNextids);
                return true;
            }
        }.call())
            throw new XError("tablesys flush fail");
	    super.flushDirties();
    }

    public static final class TableSysData implements Bean<TableSysData> {

	    public static TableSysData newBean() {
	        return new TableSysData();
        }

	    public static void marshalBean(TableSysData bean, BinaryStream os) {
	        bean.marshal(os);
        }

        public static TableSysData unmarshalBean(BinaryStream os) {
	        TableSysData bean = newBean();
	        bean.unmarshal(os);
	        return bean;
        }

        private perfect.txn.collections.XMap<String, Long> _nextids;

        public TableSysData() {
            this(perfect.txn.collections.Utils.newBeanMap(false));
        }

        public TableSysData(perfect.txn.collections.XMap<String, Long> _nextids_) {
            this._nextids = _nextids_;
        }

        @Override
        public String toString() {
            return "TableSysData{" + "nextids=" + nextids() + "}";
        }

        public perfect.txn.collections.XMap<String, Long> nextids() {
            return _nextids;
        }

        private TKey _root_;

        @Override
        public TKey getRootDirectly() {
            return _root_;
        }

        @Override
        public void setRootDirectly(TKey root) {
            _root_ = root;
        }

        @Override
        public void setChildrenRootInTxn(Transaction txn, TKey root) {
            this._nextids.setRootInTxn(txn, root);
        }

        @Override
        public void applyChildrenRootInTxn(TKey root) {
            this._nextids.applyRootInTxn(root);
        }

        @Override
        public TableSysData copy() {
            return new TableSysData(nextids().copy());
        }

        @Override
        public TableSysData noTransactionCopy() {
            return new TableSysData(_nextids.noTransactionCopy());
        }

        @Override
        public void marshal(BinaryStream _os_) {
            _os_.writeCompactUint( 1);
            _os_.writeInt( (Tag.MAP | 1));
            {
                final BinaryStream _temp_ = _os_;
                _os_ = new BinaryStream();
                this._nextids.marshal(_os_, (os, x) -> {
                    os.writeString(x);
                }, (os, x) -> {
                    os.writeLong(x);
                });
                _temp_.writeBinaryStream(_os_);
            }
        }

        @Override
        public void unmarshal(BinaryStream _os_) {
            for (int _var_num_ = _os_.readCompactUint(); _var_num_-- > 0; ) {
                final int _id_ = _os_.readInt();
                switch (_id_) {
                    case (Tag.MAP | 1): {
                        final BinaryStream _temp_ = _os_;
                        _os_ = BinaryStream.wrap(_temp_.readBytes());
                        this._nextids.unmarshal(_os_, os -> os.readString(), os -> os.readLong());
                        _os_ = _temp_;
                    }
                    break;
                    default:
                        Bean.skipUnknownField(_id_, _os_);
                }
            }
        }
    }
}
