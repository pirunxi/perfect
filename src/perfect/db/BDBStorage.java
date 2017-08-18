package perfect.db;

import com.sleepycat.je.*;
import com.sleepycat.je.Database;
import com.sleepycat.je.util.DbBackup;
import org.slf4j.Logger;
import perfect.common.Trace;
import perfect.marshal.Binary;
import perfect.txn.IStorage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;


public final class BDBStorage  {
	private final static Logger log = Trace.log;

	public static BDBStorage create(BDBConfig conf) {
		return new BDBStorage(conf);
	}
	
	private boolean closed;
	
	private final EnvironmentConfig envConf;
	private final DatabaseConfig dbConf;
	private final TransactionConfig txnConf;
	
	private final Environment env;
	
	private final String root;
	private final String backupRoot;
	private final long incrementalBackupInterval;
	private final long fullBackupInterval;
	
	public static final class DTable {
		private final Database database;
		private final Lock rlock;
		private final Lock wlock;
		public DTable(Database db, Lock r, Lock w) {
			this.database = db;
			this.rlock = r;
			this.wlock = w;
		}
		public final Database getDatabase() {
			return database;
		}
		public final Lock getRlock() {
			return rlock;
		}
		public final Lock getWlock() {
			return wlock;
		}
	}

	private final Map<Integer, DTable> databases;

	BDBStorage(BDBConfig conf) {
		this.closed = false;
		// if environment root directory not exists, we create it.
		this.root = conf.getEnvRoot();
		File fd = new File(this.root);
		if (!fd.exists()) {
			fd.mkdirs();
		}
		this.backupRoot = conf.getBackupRoot().isEmpty() ? this.root + "/backup" : conf.getBackupRoot();
		this.incrementalBackupInterval = conf.getIncrementalBackupInterval() * 1000L;
		this.fullBackupInterval = conf.getFullBackupInterval() * 1000L;

		this.envConf = new EnvironmentConfig();
		this.envConf.setAllowCreate(true);
		this.envConf.setTransactional(true);
		if(conf.getCacheSize() != 0) {
			this.envConf.setCacheSize(conf.getCacheSize());
		}
		if(conf.getEnvDurability() != null) {
			this.envConf.setDurability(conf.getEnvDurability());
		}

		this.dbConf = new DatabaseConfig();
		this.dbConf.setAllowCreate(true);
		this.dbConf.setTransactional(true);

		this.txnConf = new TransactionConfig();
		if(conf.getTxnDurability() != null) {
			this.txnConf.setDurability(conf.getTxnDurability());
		}

		this.env = new Environment(new File(root), envConf);

		this.databases = new HashMap<>();
	}

	public void start() {

	}

	private static class Storage implements IStorage {
		private final Database database;
		public Storage(Database database) {
			this.database = database;
		}

		@Override
		public byte[] find(byte[] key, int ksize) {
			DatabaseEntry dkey = new DatabaseEntry(key, 0, ksize);
			DatabaseEntry dvalue = new DatabaseEntry();
			OperationStatus status = database.get(null, dkey, dvalue, LockMode.READ_COMMITTED);
			if(status == OperationStatus.SUCCESS) {
				return dvalue.getData();
			} else {
				return null;
			}
		}

		@Override
		public void replace(byte[] key, int ksize, byte[] value, int vsize) {
			log.debug("BDBStorage.putData  key:{}", key);
			DatabaseEntry dkey = new DatabaseEntry(key, 0, ksize);
			DatabaseEntry dvalue = new DatabaseEntry(value, 0, vsize);
			OperationStatus status = database.put(null, dkey, dvalue);
		}

		@Override
		public void remove(byte[] key, int ksize) {
			DatabaseEntry dkey = new DatabaseEntry(key, 0, ksize);
			OperationStatus status = database.delete(null, dkey);
		}

		@Override
		public void walk(IWalk walker) {
			Cursor cursor = database.openCursor(null, null);
			DatabaseEntry key = new DatabaseEntry(Binary.emptyByteArray());
			DatabaseEntry value = new DatabaseEntry();
			OperationStatus status = cursor.getSearchKeyRange(key, value, LockMode.READ_COMMITTED);
			for (; status == OperationStatus.SUCCESS; status = cursor.getNext(key, value, LockMode.READ_COMMITTED)) {
				if (!walker.onRecord(key.getData(), value.getData())) break;
			}
		}
	}

	public IStorage addTable(int dbid, String dbname) {
		Database db = this.env.openDatabase(null, dbname, this.dbConf);
		log.info("database open. id:{} name:{}", dbid, dbname);
		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		this.databases.put(dbid, new DTable(db, lock.readLock(), lock.writeLock()));
		return new Storage(db);
	}

	public DTable getTable(int tableid) {
		return this.databases.get(tableid);
	}

	public Transaction getTxn() {
		return this.env.beginTransaction(null, this.txnConf);
	}

	public Binary getData(Transaction txn, Database db, Binary key, LockMode lm) {
		DatabaseEntry dkey = new DatabaseEntry(key.array());
		DatabaseEntry dvalue = new DatabaseEntry();
		OperationStatus status = db.get(txn, dkey, dvalue, lm);
		if(status == OperationStatus.SUCCESS) {
			return Binary.wrap(dvalue.getData());
		} else {
			return null;
		}
	}

	public boolean putData(Transaction txn , Database db, Binary key, Binary value) {
		log.debug("BDBStorage.putData  key:{}", key);
		DatabaseEntry dkey = new DatabaseEntry(key.array());
		DatabaseEntry dvalue = new DatabaseEntry(value.array());
		OperationStatus status = db.put(txn, dkey, dvalue);
		return (status == OperationStatus.SUCCESS);
	}

	public boolean delData(Transaction txn , Database db, Binary key) {
		log.debug("BDBStorage.delData. key:{}", key);
		DatabaseEntry dkey = new DatabaseEntry(key.array());
		OperationStatus status = db.delete(txn, dkey);
		return (status == OperationStatus.SUCCESS);
	}

	/*
	public void walk(int tableid, Binary begin, Database.Walker w) {
		DTable dTable = getTable(tableid);
//		Lock lock = dTable.getRlock();
//		lock.lock();
//		try {
			Database db = dTable.getDatabase();
			Cursor cursor = db.openCursor(null, null);
			DatabaseEntry key = new DatabaseEntry(begin.array());
			DatabaseEntry value = new DatabaseEntry();
			OperationStatus status = cursor.getSearchKeyRange(key, value, LockMode.READ_UNCOMMITTED);
			for( ; status == OperationStatus.SUCCESS ; status = cursor.getNext(key, value, LockMode.READ_UNCOMMITTED)) {
				Binary okey =  Binary.wrap(key.getData(), key.getSize());
				Binary ovalue = Binary.wrap(value.getData(), value.getSize());
				if(!w.onWalk(okey, ovalue)) break;
			}
//		} finally {
//			lock.unlock();
//		}
	}
*/

	public boolean truncateTable(int tableid) {
		DTable dTable = getTable(tableid);
		Lock lock = dTable.getWlock();
		lock.lock();
		Transaction txn = this.getTxn();
		Database db = dTable.getDatabase();
		String dbName  = db.getDatabaseName();
		try {
			db.close();
			this.env.truncateDatabase(txn, dbName, false);
			txn.commit();
		} catch (DatabaseException e) {
			log.error("BDBStorage.truncateTable fail. tableid:{}", tableid);
			log.error("BDBStorage.truncateTable excpetion:", e);
			txn.abort();
			return false;
		} finally {
			lock.unlock();
		}
		addTable(tableid, dbName);
		return true;
	}

	public void close() {
		synchronized(this) {
			log.info("BDBStorage.close begin.");
			if(this.closed) return;
			this.closed = true;
			for (Map.Entry<Integer, DTable> e : this.databases.entrySet()) {
				Database db = e.getValue().getDatabase();
				Lock lock = e.getValue().getWlock();
				int tableid = e.getKey();
				String name = db.getDatabaseName();
				lock.lock(); // never unlock this locks to forbid another operations on closed databases.
				log.info("BDBStorage.close table <{}, {}> close begin.", tableid, name);
				db.close();
				log.info("BDBStorage.close table <{}, {}> close topEnd.", tableid, name);
			}
			this.env.close();
			log.info("BDBStorage.close topEnd.");
		}
	}

	synchronized public long backup(String backupDir, long lastFileCopiedInPrevBackup) throws IOException {
		if(this.closed) return lastFileCopiedInPrevBackup;
		log.info("BDBStorage.backup begin. backupDir:{} lastFileCopiedInPrevBackup:{}", backupDir, lastFileCopiedInPrevBackup);
		File backupFile = new File(backupDir);
		if(!backupFile.exists()) {
			backupFile.mkdirs();
			log.info("backupDir:{} no exist. create it.", backupDir);
		}
	    DbBackup backupHelper = new DbBackup(this.env, lastFileCopiedInPrevBackup);

	    backupHelper.startBackup();
	    try {
	        String[] filesForBackup = backupHelper.getLogFilesInBackupSet();
	        for(String file : filesForBackup) {
	        	String src = this.root + "/" + file;
	        	String dst = backupDir + "/" + file;
	        	Files.copy(Paths.get(src), Paths.get(dst), REPLACE_EXISTING);
	        }

	        lastFileCopiedInPrevBackup = backupHelper.getLastFileInBackupSet();
	        log.info("BDBStorage.backup topEnd. backupDir:{} lastFileCopiedInPrevBackup:{}", backupDir, lastFileCopiedInPrevBackup);
	        return lastFileCopiedInPrevBackup;
	    } finally {
	       backupHelper.endBackup();
	   }
	}

	public boolean put(Map<Integer, Map<Binary, Binary>> tableDatasMap) {
//	    log.info("Storage.put tables:{} datas:{}", tableDatasMap.size(), tableDatasMap.values().stream().mapToInt(s -> s.size()).sum());
//		TreeMap<Integer, Boolean> locks = new TreeMap<Integer, Boolean>();
//		for(Integer tableid : tableDatasMap.keySet()) {
//			locks.put(tableid, false); // write lock.
//		}
		long t1 = System.currentTimeMillis();
		long t2 = System.currentTimeMillis();
//		log.info("BDBStorage.put lock cost time:{} ms", (t2 - t1));
		//Transaction txn = this.getTxn();
		try {
			for(Map.Entry<Integer, Map<Binary, Binary>> e : tableDatasMap.entrySet()) {
				Integer tableid = e.getKey();
				//lockDB(tableid, false);
                    DTable table = this.getTable(tableid);
                    Database db = table.getDatabase();
                    for (Map.Entry<Binary, Binary> e2 : e.getValue().entrySet()) {
                        // we presume value which is empty Binary means we should delete it.
                        Binary value = e2.getValue();
                        if (value.isEmpty()) {
                            this.putData(null, db, e2.getKey(), value);
                        } else {
                            this.delData(null, db, e2.getKey());
                        }
                    }
            }
			//txn.subCommit();
			long t3 = System.currentTimeMillis();
//			log.info("BDBStorage.put subCommit cost time:{}, total cost time:{}", (t3 - t2), (t3 - t1));
			return true;
		} catch (Exception e) {
			log.error("BDBStorage.put >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			log.error("BDBStorage.put ", e);
			log.error("BDBStorage.put >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			//txn.abort();
			return false;
		}
	}

	public void checkpoint() {
		// unnecessary to invoke environment.checkpoint();
		this.env.flushLog(true);
		log.info("BDBStorage.checkpoint succ.");
	}
}
