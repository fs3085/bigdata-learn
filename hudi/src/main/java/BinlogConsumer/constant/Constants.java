package BinlogConsumer.constant;

public interface Constants {
    interface CanalOperationType {
        String INSERT = "INSERT";
        String UPDATE = "UPDATE";
        String DELETE = "DELETE";
    }

    interface HudiOperationType {
        String UPSERT = "upsert";
        String INSERT = "insert";
        String DELETE = "delete";
    }

    interface HudiTableMeta {
        String PARTITION_KEY = "_partition_key";
        String VOID_DATE_PARTITION_VAL = "9999/12/30";
    }
}
