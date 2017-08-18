package perfect.db;

import com.sleepycat.je.Durability;

public final class BDBConfig {
	private String envRoot;

	private long cacheSize;
	private Durability envDurability;
	private Durability txnDurability;

	private String backupRoot;
	private int incrementalBackupInterval;
	private int fullBackupInterval;
	
	public BDBConfig() {
		this.envRoot = "_database_";
		this.cacheSize = 0;
		this.envDurability = Durability.COMMIT_NO_SYNC;
		this.txnDurability = Durability.COMMIT_NO_SYNC;
		this.backupRoot = this.envRoot + "/" + "backup";
		this.incrementalBackupInterval = 1800;
		this.fullBackupInterval = 86400;
	}

	public final String getEnvRoot() {
		return envRoot;
	}

	public final void setEnvRoot(String envRoot) {
		this.envRoot = envRoot;
	}
	
	public final String getBackupRoot() {
		return backupRoot;
	}

	public final void setBackupRoot(String backupRoot) {
		this.backupRoot = backupRoot;
	}

	public final int getIncrementalBackupInterval() {
		return incrementalBackupInterval;
	}

	public final void setIncrementalBackupInterval(int incrementalBackupInterval) {
		this.incrementalBackupInterval = incrementalBackupInterval;
	}

	public final int getFullBackupInterval() {
		return fullBackupInterval;
	}

	public final void setFullBackupInterval(int fullBackupInterval) {
		this.fullBackupInterval = fullBackupInterval;
	}

	public final long getCacheSize() {
		return cacheSize;
	}

	public final void setCacheSize(long cacheSize) {
		this.cacheSize = cacheSize;
	}

	public final Durability getEnvDurability() {
		return envDurability;
	}

	public final void setEnvDurability(Durability durability) {
		this.envDurability = durability;
	}

	public final Durability getTxnDurability() {
		return txnDurability;
	}

	public final void setTxnDurability(Durability txnDurability) {
		this.txnDurability = txnDurability;
	}
	
}
