package BinlogConsumer.config;

import org.apache.hudi.com.beust.jcommander.Parameter;

import java.io.Serializable;

/** use for hive table import to hudi table config
 */
public class HiveImport2HudiConfig extends HudiTableSaveConfig implements Serializable {
    @Parameter(names = {"--sync-hive-db-name"},description = "hive db which  will import to hudi,has no default value. eg. crm")
    private String syncHiveDb;
    @Parameter(names = {"--sync-hive-table-name"},description = "hive table which  will import to hudi,has no default value. eg. crm")
    private String syncHiveTable;
    @Parameter(names = {"--hive-base-path"},description = "hive warehouse base location in hdfs.defaut is '/user/hive/warehouse'")
    private String hiveBasePath = "/user/hive/warehouse";

    public String getSyncHiveDb() {
        return syncHiveDb;
    }

    public void setSyncHiveDb(String syncHiveDb) {
        this.syncHiveDb = syncHiveDb;
    }

    public String getSyncHiveTable() {
        return syncHiveTable;
    }

    public void setSyncHiveTable(String syncHiveTable) {
        this.syncHiveTable = syncHiveTable;
    }

    public String getHiveBasePath() {
        return hiveBasePath;
    }

    public void setHiveBasePath(String hiveBasePath) {
        this.hiveBasePath = hiveBasePath;
    }
}
