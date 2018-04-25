package com.hadoop.util.presto;

import com.hadoop.util.TBLogger;
import com.hadoop.util.common.Constant;
import com.hadoop.util.common.dbUtil.ConnUtil;
import com.hadoop.util.common.exception.WrappedException;
import com.hadoop.util.model.ColumnInfo;
import com.hadoop.util.model.ReferenceField;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sai.luo on 2017-5-18.
 */

public class PrestoOperate {
    private  static final TBLogger log = TBLogger.getInstance(PrestoOperate.class);
    private static final  PrestoClient  prestoClient = new PrestoClient();
    /**
     * 获取指定表名的表字段信息
     * @param tableName
     * @return List<String>
     * @throws Exception
     */
    public static List<ColumnInfo> getTableInfo(String tableName) throws Exception {
        List<ColumnInfo> resultList = new ArrayList<>();
        Connection con = prestoClient.getConn();
        ResultSet res1 = con.createStatement().executeQuery("describe "+tableName);
        while(res1.next()) {
            String columnName = res1.getString("Column");
            if (columnName.equals(Constant.ROW_ID)) continue;
            String columnType = res1.getString("Type");
            ColumnInfo columnInfo = new ColumnInfo(columnName,columnType);
            resultList.add(columnInfo);
        }
        log.info("getTableInfo  start time : "+ System.currentTimeMillis());


        return resultList;
    }
    /**
     * 获取指定表名的表字段信息
     * @param tableName
     * @return List<String>
     * @throws Exception
     */
    public static List<String> getTableColumn(String tableName) throws Exception {
        List<String> resultList = new ArrayList<>();
        Connection con = prestoClient.getConn();
        String result = "";
        ResultSet res1 = con.createStatement().executeQuery("describe "+tableName);
        while(res1.next()) {
            String columnName = res1.getString("Column");
            if (columnName.equals(Constant.ROW_ID)) continue;
            resultList.add(columnName);
        }
        return resultList;
    }

    /**
     * 获取生成hive表的prestoSql
     * @param tableName
     * @param columnInfoList
     * @return
     */
    public String genCreateTablesqlByColumnInfo(String tableName,List<ColumnInfo> columnInfoList){
        String sql = "create table "+tableName+"(";

        for (ColumnInfo columnInfo:columnInfoList){
            sql +=  "\""+columnInfo.getColumnName() +"\""+" " +columnInfo.getColumnType() + ",";
        }
        sql = sql.substring(0,sql.length()-1);
        sql += ") ";
        return sql;
    }
    public static void   excuteHiveql(String hiveql) throws Exception{
         prestoClient.excuteHiveql(hiveql);
    }
    /**
     * 更新表，新建第二张表，第一张表和第二张表存入第三张表
     * @param sourceTable
     * @param
     * @param referenceFields
     */
    public static void  updateTableByCreateNewTable(String sourceTable, String tableName, List<ReferenceField> referenceFields, String thirdTableName){

        try{
            String sql = genUptateTableSql(sourceTable,tableName,referenceFields,thirdTableName) ;
            if (sql==null) return;
            prestoClient.excuteHiveql(sql);


        }catch (Exception e){
            log.errer(e);
            throw new WrappedException(e.getMessage());
        }

    }

    /**
     * 没有就增加，有就更新表，新建第二张表，第一张表和第二张表存入第三张表
     * @param sourceTable
     * @param tableName
     * @param referenceFields
     */
    public static  void  addOrUpdateTableByCreateNewTable(String sourceTable,String tableName,List<ReferenceField> referenceFields,String thirdTableName){

        try{
            String sql = genAddOrUptateTableSql(sourceTable,tableName,referenceFields,thirdTableName) ;
            if (sql==null) return;
            prestoClient.excuteHiveql(sql);


        }catch (Exception e){
            log.errer(e);
            throw new WrappedException(e.getMessage());

        }

    }

    /**
     *
     * @param sourceTable 数据库原有的表
     * @param tableName  用来更新的表数据
     * @param referenceFields 关联字段
     * @param thirdTableName 会创建成第三张表
     * @return
     */
    public static String  genUptateTableSql(String sourceTable,String tableName,List<ReferenceField> referenceFields,String thirdTableName){
        try{

            List<ColumnInfo> targetColumnInfos = getTableInfo(sourceTable);
            List<ColumnInfo> columnInfos = getTableInfo(tableName);
            List<ColumnInfo> updateColomnInfos = new ArrayList<>();
            List<ColumnInfo> addColomnInfos = new ArrayList<>();
            List<String> sourceReferenceFields = new ArrayList<>();
            List<String > targetReferenceFields = new ArrayList<>();

            referenceFields.forEach(referenceField->{
                sourceReferenceFields.add(referenceField.getSourceTableReferenceField());
                targetReferenceFields.add(referenceField.getTargetTableReferenceField());
            });//

            for (int i=0;i< columnInfos.size();i++){
                if (targetColumnInfos.contains(columnInfos.get(i))&&!sourceReferenceFields.contains(columnInfos.get(i).getColumnName())){
                    //是包含字段，不是关联字段
                    updateColomnInfos.add( columnInfos.get(i));
                }else  if (!targetColumnInfos.contains(columnInfos.get(i) )&&!sourceReferenceFields.contains(columnInfos.get(i).getColumnName())) {
                    //不是包含字段，也不是关联字段
                    addColomnInfos.add(columnInfos.get(i));
                }
            }

            //更新语句

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("insert into ").append(thirdTableName)
                    /**
                     *    sourcetable 不在tablename 中的数据
                     */
                    .append("  select ")
                    .append(" ").append(getSelectColumnsThatColunmDiff(sourceTable,"t"));

            if (addColomnInfos.size()>0){
                addColomnInfos.forEach(columnInfo ->stringBuilder.append(", tt.\"").append(columnInfo.getColumnName()).append("\" ,") );
                stringBuilder.deleteCharAt(stringBuilder.length()-1);
            }

            stringBuilder.append(" from ")
                    .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"")
                    .append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                    .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" ");
            if (referenceFields.size()==2){
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");

            }

            stringBuilder.append(" where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  not in (select \"")
                    .append(referenceFields.get(0).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");
            if (referenceFields.size()==2){
                //两字段关联 ---
                stringBuilder.append(" and  t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"  not in (select \"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )")

                        .append(" union all")
                        .append("  select ")
                        .append(" ").append(getSelectColumnsThatColunmDiff(sourceTable,"t"));

                if (addColomnInfos.size()>0){
                    addColomnInfos.forEach(columnInfo ->stringBuilder.append(", tt.\"").append(columnInfo.getColumnName()).append("\" ,") );
                    stringBuilder.deleteCharAt(stringBuilder.length()-1);
                }

                stringBuilder.append(" from ")
                        .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"")
                        .append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                        .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" ");
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");
                stringBuilder.append(" where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  not in (select \"")
                        .append(referenceFields.get(0).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");
                stringBuilder.append(" and  t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"   in (select \"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )")

                        .append(" union all")

                        .append("  select ")
                        .append(" ").append(getSelectColumnsThatColunmDiff(sourceTable,"t"));

                if (addColomnInfos.size()>0){
                    addColomnInfos.forEach(columnInfo ->stringBuilder.append(", ttt.\"").append(columnInfo.getColumnName()).append("\" ,") );
                    stringBuilder.deleteCharAt(stringBuilder.length()-1);
                }

                stringBuilder.append(" from ")
                        .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"")
                        .append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                        .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" ");
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");
                stringBuilder.append(" where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"   in (select \"")
                        .append(referenceFields.get(0).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");
                stringBuilder.append(" and  t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"   in (select \"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");

            }



            stringBuilder.append(" union all")

                    /**
                     *  sourcetable 在tablename中的数据

                     */
                    .append(" select ") ;

            targetColumnInfos.forEach(columnInfo -> {
                if (updateColomnInfos.contains(columnInfo)&&!targetReferenceFields.contains(columnInfo.getColumnName())){
                    stringBuilder.append("tt.\"").append(columnInfo.getColumnName()).append("\" ,");

                }else {
                    stringBuilder.append("t.\"").append(columnInfo.getColumnName()).append("\" ,");
                }
            });
            addColomnInfos.forEach(columnInfo ->stringBuilder.append("tt.\"").append(columnInfo.getColumnName()).append("\" ,") );
            stringBuilder.deleteCharAt(stringBuilder.length()-1);
            stringBuilder.append(" from ")
                    .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                    .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField());
            if(referenceFields.size()==2){
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");

            }
            stringBuilder.append("\"  where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  = tt.\"")
                    .append(referenceFields.get(0).getSourceTableReferenceField() ).append("\"");
            if (referenceFields.size()==2){
                stringBuilder.append(" and t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");
            }



            log.info("更新数据sql语句： "+stringBuilder.toString());
            return  stringBuilder.toString() ;

        }catch (Exception e){
            log.errer(e);
            throw new WrappedException(e.getMessage());

        }
    }

    public static  String  genAddOrUptateTableSql(String sourceTable, String tableName, List<ReferenceField> referenceFields, String thirdTableName){
        try{

            List<ColumnInfo> targetColumnInfos = getTableInfo(sourceTable);
            List<ColumnInfo> columnInfos = getTableInfo(tableName);
            List<ColumnInfo> updateColomnInfos = new ArrayList<>();
            List<ColumnInfo> addColomnInfos = new ArrayList<>();
            List<String> sourceReferenceFields = new ArrayList<>();
            List<String > targetReferenceFields = new ArrayList<>();

            referenceFields.forEach(referenceField->{
                sourceReferenceFields.add(referenceField.getSourceTableReferenceField());
                targetReferenceFields.add(referenceField.getTargetTableReferenceField());
            });//

            for (int i=0;i< columnInfos.size();i++){
                if (targetColumnInfos.contains(columnInfos.get(i))&&!sourceReferenceFields.contains(columnInfos.get(i).getColumnName())){
                    //是包含字段，不是关联字段
                    updateColomnInfos.add( columnInfos.get(i));
                }else  if (!targetColumnInfos.contains(columnInfos.get(i) )&&!sourceReferenceFields.contains(columnInfos.get(i).getColumnName())) {
                    //不是包含字段，也不是关联字段
                    addColomnInfos.add(columnInfos.get(i));
                }
            }

            //更新语句

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("insert into ").append(thirdTableName)
            /**
             *    sourcetable 不在tablename 中的数据
             */
                    .append("  select ")
                    .append(" ").append(getSelectColumnsThatColunmDiff(sourceTable,"t"));


            if (addColomnInfos.size()>0){
                addColomnInfos.forEach(columnInfo ->stringBuilder.append(", tt.\"").append(columnInfo.getColumnName()).append("\" ,") );
                stringBuilder.deleteCharAt(stringBuilder.length()-1);
            }

            stringBuilder.append(" from ")
                    .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"")
                    .append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                    .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" ");
            if (referenceFields.size()==2){
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");

            }

            stringBuilder.append(" where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  not in (select \"")
                    .append(referenceFields.get(0).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");
            if (referenceFields.size()==2){
                //两字段关联 ---
                stringBuilder.append(" and  t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"  not in (select \"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )")

                        .append(" union all ")
                        .append("  select ")
                        .append(" ").append(getSelectColumnsThatColunmDiff(sourceTable,"t"));

                if (addColomnInfos.size()>0){
                    addColomnInfos.forEach(columnInfo ->stringBuilder.append(", tt.\"").append(columnInfo.getColumnName()).append("\" ,") );
                    stringBuilder.deleteCharAt(stringBuilder.length()-1);
                }

                stringBuilder.append(" from ")
                        .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"")
                        .append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                        .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" ");
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                            .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");
                stringBuilder.append(" where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  not in (select \"")
                        .append(referenceFields.get(0).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");
                stringBuilder.append(" and  t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"   in (select \"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )")

                        .append(" union all ")

                        .append("  select ")
                        .append(" ").append(getSelectColumnsThatColunmDiff(sourceTable,"t"));

                if (addColomnInfos.size()>0){
                    addColomnInfos.forEach(columnInfo ->stringBuilder.append(", ttt.\"").append(columnInfo.getColumnName()).append("\" ,") );
                    stringBuilder.deleteCharAt(stringBuilder.length()-1);
                }

                stringBuilder.append(" from ")
                        .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"")
                        .append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                        .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" ");
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");
                stringBuilder.append(" where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"   in (select \"")
                        .append(referenceFields.get(0).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");
                stringBuilder.append(" and  t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"   in (select \"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");

            }



            stringBuilder.append(" union all")

            /**
             *  sourcetable 在tablename中的数据

             */
            .append(" select ") ;

            targetColumnInfos.forEach(columnInfo -> {
                if (updateColomnInfos.contains(columnInfo)&&!targetReferenceFields.contains(columnInfo.getColumnName())){
                    stringBuilder.append("tt.\"").append(columnInfo.getColumnName()).append("\" ,");

                }else {
                    stringBuilder.append("t.\"").append(columnInfo.getColumnName()).append("\" ,");
                }
            });
            addColomnInfos.forEach(columnInfo ->stringBuilder.append("tt.\"").append(columnInfo.getColumnName()).append("\" ,") );
            stringBuilder.deleteCharAt(stringBuilder.length()-1);
            stringBuilder.append(" from ")
                    .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                    .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField());
            if(referenceFields.size()==2){
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");

            }
            stringBuilder.append("\"  where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  = tt.\"")
                    .append(referenceFields.get(0).getSourceTableReferenceField() ).append("\"");
            if (referenceFields.size()==2){
                stringBuilder.append(" and t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");
            }



           stringBuilder .append(" union all")

            /**
             *  tablename 不在sourcetable中的数据

             */
            .append(" select ") ;
            targetColumnInfos.forEach(columnInfo -> {
                if (targetReferenceFields.contains(columnInfo.getColumnName())){
                    for (int i=0;i<targetReferenceFields.size();i++){
                        if (columnInfo.getColumnName().equals(targetReferenceFields.get(i))){
                            stringBuilder.append("t.\"").append(sourceReferenceFields.get(i)).append("\" ,");
                        }
                    }

                }else {
                    if(columnInfos.contains(columnInfo)){
                        stringBuilder.append("t.\"").append(columnInfo.getColumnName()).append("\" ,");
                    }else {
                        stringBuilder.append("tt.\"").append(columnInfo.getColumnName()).append("\" ,");

                    }
                }
            });
            addColomnInfos.forEach(columnInfo ->stringBuilder.append("t.\"").append(columnInfo.getColumnName()).append("\" ,") );
            stringBuilder.deleteCharAt(stringBuilder.length()-1);
            stringBuilder.append(" from ")
                    .append(tableName).append("  t ").append("left join ").append(sourceTable).append(" tt on t.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" =")
                    .append("tt.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"");
            if (referenceFields.size()==2){
                stringBuilder.append(" left join ").append(sourceTable).append(" ttt on t.\"").append(referenceFields.get(1).getSourceTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getTargetTableReferenceField()).append("\" ");
            }
            stringBuilder.append(" where t.\"").append(referenceFields.get(0).getSourceTableReferenceField())
                    .append("\" not in ( select \"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  from ").append(sourceTable).append(" ) ");

            if (referenceFields.size()==2){
                stringBuilder.append(" and t.\"").append(referenceFields.get(1).getSourceTableReferenceField())
                        .append("\" not in ( select \"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"  from ").append(sourceTable).append(" ) ");

                stringBuilder.append(" union all ")
                        .append(" select ") ;

                targetColumnInfos.forEach(columnInfo -> {
                    if (targetReferenceFields.contains(columnInfo.getColumnName())){
                        for (int i=0;i<targetReferenceFields.size();i++){
                            if (columnInfo.getColumnName().equals(targetReferenceFields.get(i))){
                                stringBuilder.append("t.\"").append(sourceReferenceFields.get(i)).append("\" ,");
                            }
                        }

                    }else {
                        if(columnInfos.contains(columnInfo)){
                            stringBuilder.append("t.\"").append(columnInfo.getColumnName()).append("\" ,");
                        }else {
                            stringBuilder.append("tt.\"").append(columnInfo.getColumnName()).append("\" ,");

                        }
                    }
                });
                addColomnInfos.forEach(columnInfo ->stringBuilder.append("t.\"").append(columnInfo.getColumnName()).append("\" ,") );
                stringBuilder.deleteCharAt(stringBuilder.length()-1);
                stringBuilder.append(" from ")
                        .append(tableName).append("  t ").append("left join ").append(sourceTable).append(" tt on t.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" =")
                        .append("tt.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"");
                if (referenceFields.size()==2){
                    stringBuilder.append(" left join ").append(sourceTable).append(" ttt on t.\"").append(referenceFields.get(1).getSourceTableReferenceField()).append("\" = ttt.\"")
                            .append(referenceFields.get(1).getTargetTableReferenceField()).append("\" ");
                }
                stringBuilder.append(" where t.\"").append(referenceFields.get(0).getSourceTableReferenceField())
                        .append("\" not in ( select \"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  from ").append(sourceTable).append(" ) ");
                stringBuilder.append(" and t.\"").append(referenceFields.get(1).getSourceTableReferenceField())
                        .append("\"  in ( select \"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" from ").append(sourceTable).append(" ) ");


                stringBuilder.append(" union all ")
                        .append(" select ") ;

                targetColumnInfos.forEach(columnInfo -> {
                    if (targetReferenceFields.contains(columnInfo.getColumnName())){
                        for (int i=0;i<targetReferenceFields.size();i++){
                            if (columnInfo.getColumnName().equals(targetReferenceFields.get(i))){
                                stringBuilder.append("t.\"").append(sourceReferenceFields.get(i)).append("\" ,");
                            }
                        }

                    }else {
                         if(columnInfos.contains(columnInfo)){
                             stringBuilder.append("t.\"").append(columnInfo.getColumnName()).append("\" ,");
                        }else {
                             stringBuilder.append("ttt.\"").append(columnInfo.getColumnName()).append("\" ,");

                         }
                    }
                });
                addColomnInfos.forEach(columnInfo ->stringBuilder.append("t.\"").append(columnInfo.getColumnName()).append("\" ,") );
                stringBuilder.deleteCharAt(stringBuilder.length()-1);
                stringBuilder.append(" from ")
                        .append(tableName).append("  t ").append("left join ").append(sourceTable).append(" tt on t.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" =")
                        .append("tt.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"");
                if (referenceFields.size()==2){
                    stringBuilder.append(" left join ").append(sourceTable).append(" ttt on t.\"").append(referenceFields.get(1).getSourceTableReferenceField()).append("\" = ttt.\"")
                            .append(referenceFields.get(1).getTargetTableReferenceField()).append("\" ");
                }
                stringBuilder.append(" where t.\"").append(referenceFields.get(0).getSourceTableReferenceField())
                        .append("\"  in ( select \"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  from ").append(sourceTable).append(" ) ");
                stringBuilder.append(" and t.\"").append(referenceFields.get(1).getSourceTableReferenceField())
                        .append("\" not  in ( select \"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"  from ").append(sourceTable).append(" ) ");


            }


            log.info("更新数据update or add sql语句： "+stringBuilder.toString());
            return  stringBuilder.toString() ;

        }catch (Exception e){
            log.errer(e);
            throw new RuntimeException(e);

        }
    }
    /**
     * 删除目标中和源记录相符的记录
     * @param sourceTable
     * @param tableName
     * @param referenceFields
     */
    public static  void  deleteTable(String sourceTable,String tableName,List<ReferenceField> referenceFields,String thirdTableName){

        try{
            String sql = genDeleteTableSql(sourceTable,tableName,referenceFields,thirdTableName) ;
            if (sql==null) return;
            prestoClient.excuteHiveql(sql);


        }catch (Exception e){
            log.errer(e);
            throw new RuntimeException(e.getMessage());
        }

    }
    public static String  genDeleteTableSql(String sourceTable,String tableName,List<ReferenceField> referenceFields,String thirdTableName){
        try{
            //更新语句
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("create table  ").append(thirdTableName).append("  as  select ")
                    .append(" ").append(getSelectColumnsThatColunmDiff(sourceTable,"t"));

            stringBuilder.append(" from ")
                    .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"")
                    .append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                    .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" ");
            if (referenceFields.size()==2){
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");

            }

            stringBuilder.append(" where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  not in (select \"")
                    .append(referenceFields.get(0).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");
            if (referenceFields.size()==2){
                //两字段关联 ---
                stringBuilder.append(" and  t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"  not in (select \"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )")

                        .append(" union all")
                        .append("  select ")
                        .append(" ").append(getSelectColumnsThatColunmDiff(sourceTable,"t"));

                stringBuilder.append(" from ")
                        .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"")
                        .append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                        .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" ");
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");
                stringBuilder.append(" where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"  not in (select \"")
                        .append(referenceFields.get(0).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");
                stringBuilder.append(" and  t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"   in (select \"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )")

                        .append(" union all")

                        .append("  select ")
                        .append(" ").append(getSelectColumnsThatColunmDiff(sourceTable,"t"));

                stringBuilder.append(" from ")
                        .append(sourceTable).append("  t ").append("left join ").append(tableName).append(" tt on t.\"")
                        .append(referenceFields.get(0).getTargetTableReferenceField()).append("\" =")
                        .append("tt.\"").append(referenceFields.get(0).getSourceTableReferenceField()).append("\" ");
                stringBuilder.append(" left join ").append(tableName).append(" ttt on t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\" = ttt.\"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"");
                stringBuilder.append(" where t.\"").append(referenceFields.get(0).getTargetTableReferenceField()).append("\"   in (select \"")
                        .append(referenceFields.get(0).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");
                stringBuilder.append(" and  t.\"").append(referenceFields.get(1).getTargetTableReferenceField()).append("\"   in (select \"")
                        .append(referenceFields.get(1).getSourceTableReferenceField()).append("\"  from ").append(tableName).append(" )");

            }
            log.info(" 删除语句 sql :" +stringBuilder.toString());
            return  stringBuilder.toString() ;
        }catch (Exception e){
            log.errer(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据表名称、查询字段、条件、限制条数返回数据,若参数为空,请填入null;
     * @param tableName
     * @param columnList
     * @param condition
     * @param
     * @return
     */
    public static   List<List> getDataFromTable(String tableName, List<String> columnList, String condition, int lastIndex, String limit )throws Exception{
        List<List> resultList = new ArrayList<>();
        ResultSet res = null;
        String hiveql = "select ";
        if(columnList!=null&&!columnList.isEmpty()){
            for (int i = 0;i < columnList.size();i++){
                if(i != columnList.size()-1)
                    hiveql +="tt.\""+ columnList.get(i)+"\" ,";
                else{
                    hiveql +="tt.\""+ columnList.get(i)+"\" from "+tableName +" tt";
                }
            }
        }else{
            hiveql += "tt.* from "+tableName +" tt";
        }


        hiveql += " where tt.talebase_id >"+lastIndex +" "+condition;

        hiveql += " order by tt.talebase_id   " + limit;
        log.info("hiveSql : "+hiveql);
        try {
            Connection con = prestoClient.getConn();
            Statement stmt = con.createStatement();
            res = stmt.executeQuery(hiveql);
            ResultSetMetaData rsmd = res.getMetaData();
            List<String> headers = new ArrayList<>();
            List<List> data = new ArrayList<>();
            boolean hasHeaders = false ;
            while(res.next()) {
                List dataItem = new ArrayList();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (!hasHeaders){
                        headers.add(rsmd.getColumnName(i));
                    }
                    dataItem.add(res.getString(rsmd.getColumnName(i)));
                }
                hasHeaders = true ;
                data.add(dataItem);
            }
            resultList.add(headers);
            resultList.add(data);
        }catch (Exception e){
            e.printStackTrace();
        }
        return resultList;
    }
    public static   List<List> getDataFromTableBySql(String sql)throws Exception{
        List<List> resultList = new ArrayList<>();
        ResultSet res = null;
        Statement stmt = null ;
        Connection con = null ;
        try {
            con = prestoClient.getConn();
            stmt = con.createStatement();
            res = stmt.executeQuery(sql);
            ResultSetMetaData rsmd = res.getMetaData();
            List<String> headers = new ArrayList<>();
            List<List> data = new ArrayList<>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    headers.add(rsmd.getTableName(i)+"_"+rsmd.getColumnName(i));
            }
            while(res.next()) {
                List dataItem = new ArrayList();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    dataItem.add(res.getString(rsmd.getColumnName(i)));
                }
                data.add(dataItem);
            }
            resultList.add(headers);
            resultList.add(data);
        }catch (Exception e){
            log.errer(e);

        }finally {
            ConnUtil.close(res,stmt,con);
        }
        return resultList;
    }
    /**
     * 获取sql中所有的列，列名 ：表名+列名
     * @param sql
     * @return
     */
    public static  List<String > getColumnHeadersThatAddedTableName(String sql){
        ResultSet res = null;
        Statement stmt = null ;
        Connection con = null ;
        List<String> headers = new ArrayList<>();

        try {
            con = prestoClient.getConn();
            stmt = con.createStatement();
            res = stmt.executeQuery(sql);
            ResultSetMetaData rsmd = res.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    headers.add(rsmd.getTableName(i)+"_"+rsmd.getColumnName(i));
            }
        }catch (Exception e){
            log.errer(e);
        }finally {
            ConnUtil.close(res,stmt,con);
        }
        return headers;
    }

    /**
     * 将 t.* ,tt.* 这样的匹配字段，转换为t.column1,t.column2,tt.column3等 列名不同
     * @param columns
     * @param tables
     * @return
     */
    public static String getSelectColumnsThatColunmDiff(List<List<String>> columns,List<String> tables){
        StringBuilder stringBuilder = new StringBuilder();
        for (int i=0 ;i<columns.size();i++){
            for (int j=0;j<columns.get(i).size();j++){
                stringBuilder.append(tables.get(i)).append(".\"").append(columns.get(i).get(j)).append("\" ,");
            }
        }
        if (stringBuilder.length()>0){
            stringBuilder.deleteCharAt(stringBuilder.length()-1);
        }
        return stringBuilder.toString();
    }

    public static  String getSelectColumnsThatColunmDiff(String tablename,String tableNameAlias){
        StringBuilder stringBuilder = new StringBuilder();

        try {
            List<String> tableColumn = getTableColumn(tablename);

            for (int j=0;j<tableColumn.size();j++){
                stringBuilder.append(tableNameAlias).append(".\"").append(tableColumn.get(j)).append("\" ,");
            }
            if (stringBuilder.length()>0){
                stringBuilder.deleteCharAt(stringBuilder.length()-1);
            }
        }catch (Exception e){
            log.errer(e);
        }

        return stringBuilder.toString();
    }


    public static String  getColumnHeadersThatMaybeAddDigte(List<String> hearders,String column,String nextColumn,int index){
        index++;
        if (hearders.contains(nextColumn)){
          return   getColumnHeadersThatMaybeAddDigte(hearders,column,column+index,index);
        }
        return nextColumn ;
    }

    public static void main(String[] args) {
        PrestoOperate prestoOperate = new PrestoOperate();
        String sql = " insert OVERWRITE  data_center_3_b7771810f2864a8b9144720adac19a65  SELECT t.talebase_id, t.\"姓名\" ,t.\"性别\" ,t.\"年龄\" ,t.\"出生日期\" ,t.\"学历\"  from data_center_3_b7771810f2864a8b9144720adac19a65  t left join data_center_3_2461c61b11d147b3996a4b0b9e13302c tt on t.\"姓名\" = tt.\"姓名\" ";
        sql = " select t.\"姓名\" ,t.\"性别\" ,t.\"年龄\" ,t.\"出生日期\" ,t.\"学历\"  from data_center_3_3a878a86042941e69f073040603832d0  t left join data_center_3_78a4fa3f1e1542359d71333485d1aab6 tt on t.\"姓名\" =tt.\"姓名\"  where t.\"姓名\"  not in (select \"姓名\"  from data_center_3_78a4fa3f1e1542359d71333485d1aab6 ) union all select t.\"姓名\" ,tt.\"性别\" ,tt.\"年龄\" ,tt.\"出生日期\" ,tt.\"学历\"  from data_center_3_3a878a86042941e69f073040603832d0  t left join data_center_3_78a4fa3f1e1542359d71333485d1aab6 tt on t.\"姓名\" =tt.\"姓名\"  where t.\"姓名\"  = tt.\"姓名\" union all select tt.\"姓名\" ,t.\"性别\" ,t.\"年龄\" ,t.\"出生日期\" ,t.\"学历\"  from data_center_3_78a4fa3f1e1542359d71333485d1aab6  t left join data_center_3_3a878a86042941e69f073040603832d0 tt on t.\"姓名\" =tt.\"姓名\" where t.\"姓名\" not in ( select \"姓名\"  from data_center_3_3a878a86042941e69f073040603832d0 )  ";
        try {
//            List<List> dataFromTableBySql = prestoOperate.getDataFromTableBySql("select   * ,(case when month(createddate) in (1,2,3) then '一' when month(createddate) in (4,5,6) then '二' when month(createddate) in (7,8,9) then '三'  else '四' end ) as \"季度\"  from data_center_2_de9ff89e9f8240cab3d74caf091ca7f2 limit 1000 ");
//            List<List> dataFromTableBySql = prestoOperate.getDataFromTableBySql(" select \"绩效\" ,\"资质测评分\",count(2) as count  from (select  (case when ( \"资质分\"*2/3.0+\"测评分\"*1/3.0 ) >0 and (\"资质分\"*(2/3.0)+\"测评分\"*(1/3.0)) <=4 then 1 when (\"资质分\"*(2.0/3)+\"测评分\"*(1.0/3)) >4 and (\"资质分\"*(2.0/3)+\"测评分\"*(1.0/3)) <=7 then 2 else 3  end  ) \"资质测评分\",  (case when \"绩效分\" >=0 and \"绩效分\" <=3  then 1  when \"绩效分\" >3 and \"绩效分\" <=6 then 2  else 3 end ) as \"绩效\"  from data_center_2_c7e74d131a844fc29aaeb9952cd53b15) as t group by \"绩效\" ,\"资质测评分\" "  );
            List<List> dataFromTableBySql = prestoOperate.getDataFromTableBySql(" select SUM(CAST( \"年龄\" as INTEGER )) from data_center_2_72e14c1d591b4e7aac280421668f9553   ");
            log.info(dataFromTableBySql.toString());
        }catch (Exception e){
            StackTraceElement[] stackTrace = e.getStackTrace();
            for (int i=0 ; i<stackTrace.length;i++){
                log.error(stackTrace.toString());
            }
        }

        /*
        try{
            List<String> tableColumn1 = getTableColumn("data_center_3_e882292b4f984500b74ac85868a53ed2");
            List<String> tableColumn2 = getTableColumn("data_center_3_e882292b4f984500b74ac85868a53ed2");
            List<String> tableColumn3 = new ArrayList<>();
            tableColumn2.forEach(column->{
                tableColumn3.add(column+" as "+ prestoOperate.getColumnHeadersThatMaybeAddDigte(tableColumn1,column,column,0));
            });

            String selectColumn = prestoOperate.getSelectColumnsThatColunmDiff(Arrays.asList(tableColumn1,tableColumn3),Arrays.asList("t","t2"));
            String sql = " create table join_table_3 as select row_number() over( order by 1) as talebase_id ,"+selectColumn+ " from data_center_3_e882292b4f984500b74ac85868a53ed2 t left join data_center_3_e882292b4f984500b74ac85868a53ed2 t2 on t.id = t2.id ";
            prestoClient.excuteHiveql(sql);
        }catch (Exception e){
            e.printStackTrace();
            log.error(e.getMessage());
        }*/

/*        List<String> headers = new ArrayList<>();
        headers.add("column");
        headers.add("column1");
        headers.add("column2");
        Iterator<String> iterator = headers.iterator();
        while (iterator.hasNext()){
            if (iterator.next().equals("column1")){
                iterator.remove();
            }
        }
        headers.add("column3");
        String columnHeadersThatMaybeAddDigte = prestoOperate.getColumnHeadersThatMaybeAddDigte(headers, "column", "column", 0);
        headers.add(columnHeadersThatMaybeAddDigte);
        System.out.println(headers);*/
    }
    public  String createTableBySql(String sql,String tableName) throws SQLException {
    String result = null ;
    Connection con = prestoClient.getConn();
    Statement statement = con.createStatement();
    ResultSet resultSet = statement.executeQuery(sql);
    ResultSetMetaData metaData = resultSet.getMetaData();
    return result ;

    }

    public int getTotalCount(String hviesql){
        return prestoClient.getTotalCount(hviesql);
    }



        /**
         * 获取相关配置
         * @param key
         * @return
         */
    public static String getProperties(String key){
        return prestoClient.getProperties(key) ;
    }

    public List<ColumnInfo> getNeedColumnInfos(List<ColumnInfo> columnInfos){
        Iterator<ColumnInfo> iterator = columnInfos.iterator();
        while (iterator.hasNext()){
            ColumnInfo next = iterator.next();
            if (next.getColumnName().equals(Constant.ROW_ID)){
                iterator.remove();
                break;
            }
        }
        return columnInfos;
    }

}
