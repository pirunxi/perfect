package perfect.db;

import org.slf4j.Logger;
import perfect.common.Trace;
import perfect.txn.IStorage;
import perfect.txn.Table;
import perfect.txn.TableSys;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by HuangQiang on 2016/12/9.
 */
public class Database {
    private final static Logger log = Trace.log;

    private static Database ins ;

    public static Database getIns() {
        return ins;
    }

    public static void init(int serverid, String dbConfFile, List<Table<?,?>> tables) {
        if(ins != null)
            throw new XError("database has opened");
        ins = new Database(serverid, dbConfFile, tables);
    }


    private final int serverid;
    private final BDBConfig conf;
    private final BDBStorage database;
    private final List<Table<?,?>> tables;

    private Checkpoint checkpoint;

    public Database(int serverid, String dbConfFile, List<Table<?,?>> tables) {
        this.serverid = serverid;
        this.conf = new BDBConfig();
        this.database = BDBStorage.create(conf);
        this.tables = new ArrayList<>(tables);
        this.tables.add(TableSys.getTable());
    }

    public void start() {
        for(Table<?,?> table : tables) {
            IStorage storage = database.addTable(table.getId(), table.getName());
            perfect.txn.Table.Conf tconf = new perfect.txn.Table.Conf(storage, 10000, 60, 60 * 30);
            table.open(tconf);
        }

        database.start();

        TableSys.getTable().init(serverid);

        checkpoint = new Checkpoint();
        checkpoint.start();
    }

    public long nextid(Table<?,?> table) {
        return TableSys.getTable().nextid(table.getName(), serverid, 1 << 12);
    }

    public void checkpoint() {
        log.info("checkpoint begin.");
        long beginTime = System.currentTimeMillis();
        for(Table<?,?> table : tables) {
            table.flushDirties();
        }
        long endTime = System.currentTimeMillis();
        log.info("checkpoint end. cost time {} ms", (endTime - beginTime));
        database.checkpoint();
    }

    public void shrink() {
        log.info("shrink begin.");
        long beginTime = System.currentTimeMillis();
        for(Table<?,?> table : tables) {
            table.shrink();
        }
        long endTime = System.currentTimeMillis();
        log.info("shrink end. cost time {} ms", (endTime - beginTime));
    }

    public void close() {
        checkpoint.close();
        database.close();
    }
}
