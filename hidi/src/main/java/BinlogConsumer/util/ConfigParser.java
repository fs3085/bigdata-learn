package BinlogConsumer.util;

import BinlogConsumer.config.CanalKafkaImport2HudiConfig;
import BinlogConsumer.config.HiveImport2HudiConfig;
import BinlogConsumer.config.HiveMetaSyncConfig;
import BinlogConsumer.config.HudiTableSaveConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.com.beust.jcommander.JCommander;

public class ConfigParser {
    public static CanalKafkaImport2HudiConfig parseHudiDataSaveConfig(String[] args) {
        CanalKafkaImport2HudiConfig config = new CanalKafkaImport2HudiConfig();
        JCommander.newBuilder()
                .addObject(config)
                .build()
                .parse(args);
        if (StringUtils.isBlank(config.getKafkaGroup())) {
            config.setKafkaGroup(buildKafkaGroup(config));
        }
        setDefaultValueForHudiSave(config);
        return config;
    }

    public static HiveMetaSyncConfig parseHiveMetaSyncConfig(String[] args) {
        HiveMetaSyncConfig config = new HiveMetaSyncConfig();
        JCommander.newBuilder()
                .addObject(config)
                .build()
                .parse(args);
        return config;
    }

    public static HiveImport2HudiConfig parseHiveImport2HudiConfig(String[] args) {
        HiveImport2HudiConfig config = new HiveImport2HudiConfig();
        JCommander.newBuilder()
                .addObject(config)
                .build()
                .parse(args);
        setDefaultValueForHudiSave(config);
        return config;
    }


    private static void setDefaultValueForHudiSave(HudiTableSaveConfig config) {
        if (StringUtils.isBlank(config.getStoreTableName())) {
            config.setStoreTableName(buildHudiStoreTableName(config));
        }
        if (StringUtils.isBlank(config.getRealSavePath())) {
            config.setRealSavePath(buildRealSavePath(config));
        }
    }

    private static String buildKafkaGroup(CanalKafkaImport2HudiConfig config) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("hudi_");
        stringBuilder.append(config.getMappingMysqlDbName());
        stringBuilder.append("__");
        stringBuilder.append(config.getMappingMysqlTableName());
        return stringBuilder.toString();
    }


    private static String buildHudiStoreTableName(HudiTableSaveConfig config) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(config.getMappingMysqlDbName());
        stringBuilder.append("__");
        stringBuilder.append(config.getMappingMysqlTableName());
        return stringBuilder.toString();
    }

    private static String buildRealSavePath(HudiTableSaveConfig config) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(config.getBaseSavePath());
        if (!config.getBaseSavePath().endsWith("/")) {
            stringBuilder.append("/");
        }
        stringBuilder.append(config.getStoreTableName());
        return stringBuilder.toString();
    }
}
