package BinlogConsumer.util;

import BinlogConsumer.bo.CanalObject;
import BinlogConsumer.bo.HudiHandleObject;
import BinlogConsumer.constant.Constants;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;

import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class CanalDataParser {
    private static Set<String> allowedOparation = Sets.newHashSet(Constants.CanalOperationType.INSERT, Constants.CanalOperationType.UPDATE, Constants.CanalOperationType.DELETE);
    private static Map<String, String> canalOperationMapping2HudiOperation = Maps.newHashMap();

    static {
        canalOperationMapping2HudiOperation.put(Constants.CanalOperationType.INSERT,Constants.HudiOperationType.UPSERT );
        canalOperationMapping2HudiOperation.put(Constants.CanalOperationType.UPDATE,Constants.HudiOperationType.UPSERT );
        canalOperationMapping2HudiOperation.put(Constants.CanalOperationType.DELETE,Constants.HudiOperationType.DELETE );
    }

    /**
     * 将canal原始数据转化成Hudi可以使用的数据信息
     *
     * @param originalCanalData
     * @return
     * @throws IOException
     */
    public static HudiHandleObject parse(String originalCanalData) throws IOException {
        Preconditions.checkNotNull(originalCanalData, "canal data can not be null");
        //直接一次性解析成对象, 从对象中提取出想要的东西
        //如果不为自己关注的操作类型，直接返回
        //为自己操作的类型，构建对应对象
        CanalObject canalObject = JsonUtils.getObjectMapper().readValue(originalCanalData, CanalObject.class);
        Preconditions.checkNotNull(canalObject, "canal object can not be null");
        Preconditions.checkNotNull(canalObject.getTable(), "canal op type  can not be null ");
        if (!allowedOparation.contains(canalObject.getType())) {
            return null;
        }
        HudiHandleObject result = new HudiHandleObject();
        result.setOperationType(canalOperationMapping2HudiOperation.get(canalObject.getType()));
        result.setDatabase(canalObject.getDatabase());
        result.setTable(canalObject.getTable());
        result.setData(canalObject.getData());
        return result;
    }



    /**
     * 将canal中的map数据，转化成json List
     *
     * @param partitionDateField
     * @return
     */
    public static List<String> buildJsonDataString(List<Map<String,String>> data, String partitionDateField) {
        //找出其中的数据
        //从type中，找到其对应的数据类型
        //将这其value转化成对应对象
        return data.stream()
                .map(dataMap -> addHudiRecognizePartition(dataMap,partitionDateField))
                .map(dataMap->toLowerCaseKeyMap(dataMap))
                .map(stringObjectMap -> JsonUtils.toJson(stringObjectMap))
                .collect(Collectors.toList());
    }


    private static Map<String, String> toLowerCaseKeyMap(Map<String, String> dataMap) {
        Map<String, String> result = Maps.newHashMapWithExpectedSize(dataMap.size());
        for(Map.Entry<String, String> entry:dataMap.entrySet()) {
            result.put(entry.getKey().toLowerCase(),entry.getValue());
        }
        return result;
    }

    /** 将最原始数据中指定的创建时间戳加工成，分区元数据字段：Constants.HudiTableMeta.PARTITION_KEY
     * @param dataMap
     * @param partitionDateField
     * @return
     */
    private static Map<String,String> addHudiRecognizePartition(Map<String, String> dataMap, String partitionDateField) {
        String partitionOriginalValue = dataMap.get(partitionDateField);
        if(StringUtils.isBlank(partitionOriginalValue)) {
            log.error("分区日期为空，改为9999/12/30");
            dataMap.put(Constants.HudiTableMeta.PARTITION_KEY, Constants.HudiTableMeta.VOID_DATE_PARTITION_VAL);
            return dataMap;
        }
        Preconditions.checkArgument(StringUtils.isNotBlank(partitionOriginalValue),"partition value can not be null");
        String hudiPartitionFormatValue;
        if(DateUtils.isValidDateTime(partitionOriginalValue)) {
            hudiPartitionFormatValue = DateUtils.dateStringFormat(partitionOriginalValue, DateUtils.DATE_FORMAT_YYYY_MM_DD_hh_mm_ss, DateUtils.DATE_FORMAT_YYYY_MM_DD_SLASH);
        } else if(DateUtils.isValidDate(partitionOriginalValue)) {
            hudiPartitionFormatValue = DateUtils.dateStringFormat(partitionOriginalValue,DateUtils.DATE_FORMAT_YYYY_MM_DD,DateUtils.DATE_FORMAT_YYYY_MM_DD_SLASH);
        } else {
            throw new RuntimeException("partition field must be any type of [datetime,timestamp,date] ");
        }
        dataMap.put(Constants.HudiTableMeta.PARTITION_KEY,hudiPartitionFormatValue);
        return dataMap;
    }
}
