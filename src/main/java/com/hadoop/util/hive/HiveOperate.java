package com.hadoop.util.hive;



import com.hadoop.util.TBLogger;
import com.hadoop.util.common.Constant;
import com.hadoop.util.common.UUIDUtil;
import com.hadoop.util.common.enums.ImportOperatorType;
import com.hadoop.util.model.ColumnInfo;
import com.hadoop.util.model.ReferenceField;
import com.hadoop.util.presto.PrestoOperate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by sai.luo on 2017-5-18.
 */
public class HiveOperate {
    private  static final TBLogger log = TBLogger.getInstance(HiveOperate.class);
    private static final HiveJdbcClient hiveJdbcClient = new HiveJdbcClient();
    private static   RemoteOpera remoteOpera = new RemoteOpera();



    public static List<ColumnInfo> getTableColumnInfo(String tableName) throws Exception {
        List<ColumnInfo> resultList = new ArrayList<ColumnInfo>();
        List<Map<String,String>> resultMapList = hiveJdbcClient.describeTable(tableName);
        for(int i=0;i<resultMapList.size();i++){
            if (resultMapList.get(i).get("col_name").equals(Constant.ROW_ID))continue;
            resultList.add(new ColumnInfo(resultMapList.get(i).get("col_name"),resultMapList.get(i).get("data_type")));
        }
        return resultList;
    }



    /**
     * 获取生成hive表的hiveql
     * @param tableName
     * @param columnInfoList
     * @return
     */
    public static String genCreateTablesqlByColumnInfo(String tableName,List<ColumnInfo> columnInfoList,String split){
        String sql = "create table "+tableName+"(";

        for (ColumnInfo columnInfo:columnInfoList){
            sql +=  "`"+columnInfo.getColumnName() +"`"+"  " +columnInfo.getColumnType() + ",";
        }
        sql = sql.substring(0,sql.length()-1);
        sql += ") row format delimited fields terminated by '"+split+"'";
        return sql;
    }
    public static void   createTable(String sourceTable, String tableName, String thirdTableName, String split, List<ReferenceField> referenceFields){
        try{

            List<ColumnInfo> sourceColumnInfos = getTableColumnInfo(sourceTable);
            List<ColumnInfo> columnInfos = getTableColumnInfo(tableName);
            List<ColumnInfo> thirdColomnInfos = new ArrayList<>(sourceColumnInfos);
            List<String> sourceReferenceFields = new ArrayList<>();
            referenceFields.forEach(referenceField->{
                sourceReferenceFields.add(referenceField.getSourceTableReferenceField());
            });
            for (int i=0;i< columnInfos.size();i++){
                if (sourceReferenceFields.contains(columnInfos.get(i).getColumnName())) continue; //如果是关联字段则 跳过，不创建改字段
                if (!sourceColumnInfos.contains(columnInfos.get(i))){
                    thirdColomnInfos.add(columnInfos.get(i));
                }
            }
            String createTableSql = genCreateTablesqlByColumnInfo(thirdTableName, thirdColomnInfos,split);
            String info =  hiveJdbcClient.excuteHiveql(createTableSql);
            log.info("更新后的表结构: "+info);

        }catch (Exception e){
            log.errer(e);
            throw new RuntimeException(e.getCause().getLocalizedMessage());
        }
    }


    /**
     * 文件上传并且导入到hive中
     * @param tableName
     * @param sourceTable 表更新关联时的原表
     * @param tmpFile
     * @param columnMapList
     * @param referenceFields 表更新关联字段
     */
    public static String  executeHiveOperate(String tableName,String sourceTable,String tmpFile,List<ColumnInfo> columnMapList,List referenceFields,String split,int operateType,String localFilePath)throws Exception{
        String result = tableName ;
        //上传服务器
        remoteOpera.transferFile(hiveJdbcClient.getProperty("remote.ip"),hiveJdbcClient.getProperty("remote.username"),hiveJdbcClient.getProperty("remote.password"),localFilePath+tmpFile,hiveJdbcClient.getProperty("remote.filepath"));

        //根据表名 表头 拼接sql
        String createTableSql = HiveOperate.genCreateTablesqlByColumnInfo(tableName,columnMapList,split);
        log.info(createTableSql);
        try {
            //建表 hive
            String info =  hiveJdbcClient.excuteHiveql(createTableSql);
            log.info(info);
            //导入到HDFS 文件系统 shell hadoop
        /*    String cmd = "hadoop fs -put " + hiveJdbcClient.getProperty("remote.filepath") + "/" + tmpFile + " /tmp/hive/root/" + tmpFile;
            remoteOpera.excuteCmd(hiveJdbcClient.getProperty("remote.ip"), hiveJdbcClient.getProperty("remote.username"), hiveJdbcClient.getProperty("remote.password"), cmd);
            //导入数据 hive
            String loadStr = "load data inpath '/tmp/hive/root/" + tmpFile + "' into table " + tableName;
      */
            String loadStr = "load data local inpath '/root/" + tmpFile + "' into table " + tableName;
            hiveJdbcClient.excuteHiveql(loadStr);
            //shell 删除临时文件
            String cmd1 = "rm -rf "+hiveJdbcClient.getProperty("remote.filepath") + "/" + tmpFile;
            remoteOpera.excuteCmd(hiveJdbcClient.getProperty("remote.ip"), hiveJdbcClient.getProperty("remote.username"), hiveJdbcClient.getProperty("remote.password"), cmd1);

        } catch (Exception e) {
            e.printStackTrace();
            log.errer(e);
            throw  new RuntimeException(e.getCause().getLocalizedMessage());
        }
        return  result ;
    }


}
