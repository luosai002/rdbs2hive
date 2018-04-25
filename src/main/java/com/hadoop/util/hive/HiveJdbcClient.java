package com.hadoop.util.hive;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

/**
 * hivejdbc连接类
 * @author suntree.xu
 *
 */
@Repository
public class HiveJdbcClient {


  public static void main(String[] args) throws Exception {
      HiveJdbcClient hiveJdbcClient = new HiveJdbcClient();


      List<String> goods2 = hiveJdbcClient.getTableInfo("goods2");
      for (String s : goods2) {
          System.out.println(s);
      }
      List<Map<String, String>> dataFromTable = hiveJdbcClient.getDataFromTable("goods2", Arrays.asList("id", "name"), null, null);
      for (Map<String, String> map : dataFromTable) {
          map.forEach((key,value)->{
              System.out.println(key+":"+value);
          });
      }


//      System.out.println(str);
  }
  
  /**
   * 获取jdbc连接
   * @return Connection
   */
  public static Connection getConn(){
	  Connection con = null;
	  try {
	      Class.forName( getProperties("hive.driverName"));
	      con = DriverManager.getConnection(getProperties("hive.connection.url"),getProperties("remote.username"),getProperties("remote.password"));
	    } catch (Exception e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    }
	  return con;
  }
  
 /**
  * 获取指定表名的表字段信息
  * @param tableName
  * @return List<String>
  * @throws Exception
  */
  public List<String> getTableInfo(String tableName) throws Exception {
      List<String> resultList = new ArrayList<String>();
      List<Map<String,String>> resultMapList = describeTable(tableName);
      for(int i=0;i<resultMapList.size();i++){
          resultList.add(resultMapList.get(i).get("col_name"));
      }
      return resultList;
  }


    public static List<Map<String,String>> describeTable(String tableName){
        List<Map<String,String>> resultList = new ArrayList<Map<String,String>>();
        Connection con = getConn();
        ResultSet res = null;
        String hiveql = "describe "+tableName;
        try {
            Statement stmt = con.createStatement();
            res = stmt.executeQuery(hiveql);
            ResultSetMetaData rsmd = res.getMetaData();
            while(res.next()) {
                Map<String,String> map = new HashMap<>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(rsmd.getColumnName(i), res.getString(rsmd.getColumnName(i)));
                }
                resultList.add(map);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return resultList;
    }

    /**
     * 根据表名称、查询字段、条件、限制条数返回数据,若参数为空,请填入"";
     * @param tableName
     * @param columnList
     * @param condition
     * @param limitInfo
     * @return
     */
    public List<Map<String,String>> getDataFromTable(String tableName, List<String> columnList, String condition,String limitInfo){
        List<Map<String,String>> resultList = new ArrayList<Map<String,String>>();
        Connection con = getConn();
        ResultSet res = null;
        String hiveql = "select ";
        if(columnList !=null&&columnList.size() > 0){
            for (int i = 0;i < columnList.size();i++){
                if(i != columnList.size()-1)
                    hiveql += columnList.get(i)+",";
                else
                    hiveql += columnList.get(i);
            }
        }else{
            hiveql += "* ";
        }
       hiveql += " from " + tableName;
       if(condition!=null&&!condition.equals("")){
           hiveql += " where "+condition;
       }
        if(limitInfo!=null&&!limitInfo.equals("")){
            hiveql += " " + limitInfo;
        }
        try {
            Statement stmt = con.createStatement();
            res = stmt.executeQuery(hiveql);
            ResultSetMetaData rsmd = res.getMetaData();
            while(res.next()) {
                Map<String,String> map = new HashMap<>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(rsmd.getColumnName(i), res.getString(rsmd.getColumnName(i)));
                }
                resultList.add(map);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return resultList;
    }

    public String excuteLoadData(String loadStr){
        String result = "";
        Connection con = getConn();
        //ResultSet res = null;
        try {
            Statement stmt = con.createStatement();
            ResultSet res = stmt.executeQuery(loadStr);
        } catch (Exception e){
                result = "执行失败："+loadStr;
                e.printStackTrace();
            }
        return result;
    }

    /**
     * 仅执行hiveql，不返回数据，只返回成功失败，比如执行创建表，加载数据等
     * @param hiveql
     * @return
     * @throws Exception
     */
    public String excuteHiveql(String hiveql) throws Exception{
        String result = "";
        Connection con = getConn();
        //ResultSet res = null;
       try {
           Statement stmt = con.createStatement();
           int bool = stmt.executeUpdate(hiveql);
           result = "执行成功："+hiveql;
       }catch (Exception e){
           result = "执行失败："+hiveql;
           e.printStackTrace();
       }
        return result;
    }

    public int queryCount(String tableName){
        int sum = 0;
        Connection con = getConn();
        try {
            Statement stmt = con.createStatement();
            ResultSet res = stmt.executeQuery("select count(*) from "+tableName);
            while(res.next()){
                sum = res.getInt(1);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return sum;
    }

    /**
     * 获取生成hive表的hiveql
     * hive表默认全部都是string类型，若有其他类型的需要另外封装方法
     * @param tableName
     * @param columnList
     * @return
     */
    public String genCreateTablesql(String tableName,List<String> columnList){
        String sql = "create table "+tableName+"(";
        for(int i = 0;i<columnList.size();i++){
            if(columnList.get(i).equals("")){
               continue;
            }
            if(i != columnList.size()-1){
                sql += columnList.get(i) + "  string,";
            }else {
                sql += columnList.get(i) + "  string";
            }
        }
        sql += ") row format delimited fields terminated by ','";
        return sql;
    }

    /**
     * 获取生成hive表的hiveql
     * @param tableName
     * @param columnMapList
     * @return
     */
    public String genCreateTablesqlByColumnAndType(String tableName,List<Map<String,String >> columnMapList){
        String sql = "create table "+tableName+"(";

        for (Map<String,String> map:columnMapList){
            Set<String> keys = map.keySet();
            for (String key :keys){
                sql +=  "`"+key +"`"+" " +map.get(key) + ",";
            }
        }
        sql = sql.substring(0,sql.length()-1);
        sql += ") row format delimited fields terminated by ','";
        return sql;
    }




/*    public String genCreateModelTablesql(String tableName,List<String> columnList){
        List<String> basicList = metaDataService.getBasicList();
        String sql = "create table "+tableName+"(";
        for(int k = 0;k<basicList.size();k++){
            sql += basicList.get(k)+"   string,";
        }
        for(int i = 0;i<columnList.size();i++){
            if(columnList.get(i).equals("")){
                continue;
            }
            if(i != columnList.size()-1){
                sql += columnList.get(i) + "  bigint,";
            }else {
                sql += columnList.get(i) + "  bigint";
            }
        }
        sql += ") row format delimited fields terminated by ','";
        return sql;
    }*/

    public String genQueryDatasql(String tableName,List<String> columnList){
        String sql = "select ";
        if(columnList.size() == 0) {
            sql += "*";
        }else {
            for (int i = 0; i < columnList.size(); i++) {
                if (i != columnList.size() - 1) {
                    sql += columnList.get(i) + ",";
                } else {
                    sql += columnList.get(i);
                }
            }
        }
        sql += " from "+tableName;
        return sql;
    }

    public boolean isTableDataExist(String tableName) {
        Connection con = getConn();
        ResultSet res =null;
        String str = "";
        try {
            Statement stmt = con.createStatement();
            res = stmt.executeQuery("select * from "+tableName+" limit 1");
            while(res.next()){
              str = res.getString(1);
            }
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return !str.equals("");
        //return false;
    }

    /**
     * 获取相关配置
     * @param key
     * @return
     */
    public static String getProperties(String key){
        String value = "";
        Properties prop = new Properties();
        InputStream in = HiveJdbcClient.class.getResourceAsStream("/hiveConfiguration.properties");
        try {
            prop.load(in);
            value = prop.getProperty(key).trim();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }

    /**
     * 获取相关配置(公用)
     * @param key
     * @return
     */
    public  String getProperty(String key){
        String value = "";
        Properties prop = new Properties();
        InputStream in = HiveJdbcClient.class.getResourceAsStream("/hiveConfiguration.properties");
        try {
            prop.load(in);
            value = prop.getProperty(key).trim();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {

        }
        return value;
    }
    class testThread extends Thread{
        public void run()
        {
            System.out.println("testThread run");
            try{
                sleep(10000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

/*    public String  genCleanSql(List<RuleGroup> list,String cleanTableName,String accessTableName){
        String sql = "insert overwrite table "+cleanTableName+" select * from "+accessTableName + " where (" ;
        for(int i = 0 ;i < list.size(); i++){
            for(int j = 0; j< list.get(i).getRules().size(); j++){
                if(j<list.get(i).getRules().size()-1) {
                    String str = list.get(i).getGroupId()<7?" or ":" and ";
                    if (list.get(i).getRules().get(j).getRuleOpera().equals("like")) {
                        sql += MetadataHelper.getEnByCh(list.get(i).getRules().get(j).getRuleColumn()) + " like '%" + list.get(i).getRules().get(j).getRuleValue() +"%'"+str ;
                    } else if (list.get(i).getRules().get(j).getRuleOpera().equals("reg")) {
                        sql += MetadataHelper.getEnByCh(list.get(i).getRules().get(j).getRuleColumn()) + " rlike '(" + list.get(i).getRules().get(j).getRuleValue() + ")'"+str;
                    } else if (list.get(i).getRules().get(j).getRuleColumn().contains("length")) {
                        sql += " length(name)" + list.get(i).getRules().get(j).getRuleOpera() + list.get(i).getRules().get(j).getRuleValue() +str;
                    } else {
                        sql += MetadataHelper.getEnByCh(list.get(i).getRules().get(j).getRuleColumn()) + list.get(i).getRules().get(j).getRuleOpera() + list.get(i).getRules().get(j).getRuleValue() + str;
                    }

                }else{
                    if (list.get(i).getRules().get(j).getRuleOpera().equals("like")) {
                        sql += MetadataHelper.getEnByCh(list.get(i).getRules().get(j).getRuleColumn()) + " like '%" + list.get(i).getRules().get(j).getRuleValue() + "%' ";
                    } else if (list.get(i).getRules().get(j).getRuleOpera().equals("reg")) {
                        sql += MetadataHelper.getEnByCh(list.get(i).getRules().get(j).getRuleColumn()) + " rlike '(" + list.get(i).getRules().get(j).getRuleValue() + ")' ";
                    } else if (list.get(i).getRules().get(j).getRuleColumn().contains("length")) {
                        sql += " length(name)" + list.get(i).getRules().get(j).getRuleOpera() + list.get(i).getRules().get(j).getRuleValue();
                    } else {
                        sql += MetadataHelper.getEnByCh(list.get(i).getRules().get(j).getRuleColumn()) + list.get(i).getRules().get(j).getRuleOpera() + list.get(i).getRules().get(j).getRuleValue();
                    }
                }
            }
            if(i<list.size()-1) {
                sql += ") or( ";
            }
        }
        sql += ")";
        return sql;
    }*/

   /* public String genSampleQuery(String sampleTableName,String cleanTableName,Map<String,List<String>> ruleMap){
        String hiveql = "insert overwrite table "+sampleTableName+" select * from "+cleanTableName+" where ";
        for(String key:ruleMap.keySet()){
            if(ruleMap.get(key).size()==0){
                continue;
            }
            hiveql += "(";
            for(int i = 0;i < ruleMap.get(key).size();i ++){
                if(i < ruleMap.get(key).size()-1){
                    if(ruleMap.get(key).get(i).split("-").length>1){
                        hiveql += "(industry='"+ruleMap.get(key).get(i).split("-")[0]+"' and ";
                        hiveql += MetadataHelper.getEnByCh(key)+"='"+ruleMap.get(key).get(i).split("-")[1]+"') or ";
                    }else{
                        hiveql += MetadataHelper.getEnByCh(key)+"='"+ruleMap.get(key).get(i)+"' or ";
                    }
                }else{
                    if(ruleMap.get(key).get(i).split("-").length>1){
                        hiveql += "(industry='"+ruleMap.get(key).get(i).split("-")[0]+"' and ";
                        hiveql += MetadataHelper.getEnByCh(key)+"='"+ruleMap.get(key).get(i).split("-")[1]+"')";
                    }else{
                        hiveql += MetadataHelper.getEnByCh(key)+"='"+ruleMap.get(key).get(i)+"'";
                    }
                }
            }
            hiveql += ") and";
        }
        if(hiveql.endsWith("where ")){
            hiveql = hiveql.substring(0,hiveql.lastIndexOf("where"));
        }else{
            hiveql = hiveql.substring(0,hiveql.lastIndexOf("and"));
        }
        return hiveql;
    }*/

/*    public String genModelHiveql(String sampleTableName,String modelTableName,List<String> columnList){
        List<String> basicList = metaDataService.getBasicList();
        columnList.removeAll(basicList);
        String hiveql = "insert overwrite table "+modelTableName+" select ";
        for(int i=0;i<basicList.size();i++){
            hiveql += basicList.get(i)+",";
        }
        for(int i=0;i<columnList.size();i++){
           hiveql += columnList.get(i)+"*10,";
            //hiveql += columnList.get(i)+",";
        }
        hiveql = hiveql.substring(0,hiveql.lastIndexOf(","));
        hiveql += " from "+sampleTableName;
        return hiveql;
    }*/



    public String genDataResult(String resultTableName,String dataModelTableName,List<String> columnList,List<String> conditionList){
        String result = "insert overwrite table " + resultTableName;
        for(int i = 0;i< conditionList.size();i ++){
            result +=  " select industry,crowd,com_function,";
            if(conditionList.get(i).equals("平均分")){
                result += "'平均分',";
                for(int j = 0;j < columnList.size(); j ++){
                    if(columnList.get(j).equals("")){
                        continue;
                    }
                    if(j<columnList.size()-1) {
                        result += "round(avg(" + columnList.get(j) + ")/10,1) as "+columnList.get(j)+",";
                    }else{
                        result += "round(avg(" + columnList.get(j) + ")/10,1) as "+columnList.get(j);
                    }
                }
                result += " from "+dataModelTableName+" group by industry,crowd,com_function,'平均分' union all ";
            }else if(conditionList.get(i).equals("25分位")){
                result += "'25分位',";
                for(int j = 0;j < columnList.size(); j ++){
                    if(columnList.get(j).equals("")){
                        continue;
                    }
                    if(j<columnList.size()-1) {
                        result += "round(percentile(" + columnList.get(j) + ",0.25)/10,1) as "+columnList.get(j)+",";
                    }else{
                        result += "round(percentile(" + columnList.get(j) + ",0.25)/10,1) as "+columnList.get(j);
                    }
                }
                result += " from "+dataModelTableName+" group by industry,crowd,com_function,'25分位' union all ";
            }else if(conditionList.get(i).equals("50分位")){
                result += "'50分位',";
                for(int j = 0;j < columnList.size(); j ++){
                    if(columnList.get(j).equals("")){
                        continue;
                    }
                    if(j<columnList.size()-1) {
                        result += "round(percentile(" + columnList.get(j) + ",0.50)/10,1) as "+columnList.get(j)+",";
                    }else{
                        result += "round(percentile(" + columnList.get(j) + ",0.50)/10,1) as "+columnList.get(j);
                    }
                }
                result += " from "+dataModelTableName+" group by industry,crowd,com_function,'50分位' union all ";
            }else if(conditionList.get(i).equals("75分位")){
                result += "'75分位',";
                for(int j = 0;j < columnList.size(); j ++){
                    if(columnList.get(j).equals("")){
                        continue;
                    }
                    if(j<columnList.size()-1) {
                        result += "round(percentile(" + columnList.get(j) + ",0.75)/10,1) as "+columnList.get(j)+",";
                    }else{
                        result += "round(percentile(" + columnList.get(j) + ",0.75)/10,1) as "+columnList.get(j);
                    }
                }
                result += " from "+dataModelTableName+" group by industry,crowd,com_function,'75分位' union all ";
            }else if(conditionList.get(i).equals("方差")){
                result += "'方差',";
                for(int j = 0;j < columnList.size(); j ++){
                    if(columnList.get(j).equals("")){
                        continue;
                    }
                    if(j<columnList.size()-1) {
                        result += "covar_pop(round(" + columnList.get(j) + "/10,1)) as "+columnList.get(j)+",";
                    }else{
                        result += "covar_pop(round(" + columnList.get(j) + "/10,1)) as "+columnList.get(j);
                    }
                }
                result += " from "+dataModelTableName+" group by industry,crowd,com_function,'方差' union all ";
            }else if(conditionList.get(i).equals("标准差")) {
                result += "'标准差',";
                for (int j = 0; j < columnList.size(); j++) {
                    if(columnList.get(j).equals("")){
                        continue;
                    }
                    if (j < columnList.size() - 1) {
                        result += "stddev_pop(round(" + columnList.get(j) + "/10,1)) as " + columnList.get(j) + ",";
                    } else {
                        result += "stddev_pop(round(" + columnList.get(j) + "/10,1)) as " + columnList.get(j);
                    }
                }
                result += " from " + dataModelTableName + " group by industry,crowd,com_function,'标准差' union all ";
            }
        }
        result = result.substring(0,result.lastIndexOf("union all"));
        result += " order by industry,crowd,com_function";
        return result;
    }



}