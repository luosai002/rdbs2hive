package com.hadoop.util.presto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

/**
 * Created by suntree.xu on 2017-3-21.
 */
@Repository
public class PrestoClient {
    private static Logger log = LoggerFactory.getLogger(PrestoClient.class);
    private static DataSource ds ;
    public static void main(String[] args) throws ClassNotFoundException,
            SQLException {
        //connect();
        PrestoClient prestoClient = new PrestoClient();
        List<List> resultList = new ArrayList<>() ;
        try{
//            prestoClient.excuteHiveql(" show tables");
//            prestoClient.excuteHiveql(" alter table data_center_2_c7e74d131a844fc29aaeb9952cd53b15 change \"年龄\" \"年龄\" string ");
            List<Map<String, String>> data_center_2_a73013989ffe49e98e6d271a9b1ed092 = prestoClient.getTableInfo("data_center_2_72e14c1d591b4e7aac280421668f9553");
            System.out.println(data_center_2_a73013989ffe49e98e6d271a9b1ed092);
        }catch (Exception e){
            e.printStackTrace();
            log.error(e.getMessage());
        }

    }

    //*****************************************************************************************************

    /**
     * 仅执行hiveql，不返回数据，只返回成功失败，比如执行创建表，加载数据等
     * @param hiveql
     * @return
     * @throws Exception
     */
    public void excuteHiveql(String hiveql) throws Exception{
        String result = "";
        Connection con = getConn();
        //ResultSet res = null;
        try {
            Statement stmt = con.createStatement();
            int bool = stmt.executeUpdate(hiveql);
            result = "执行成功："+hiveql;
            log.info(result);

        }catch (Exception e){
            result = "执行失败："+hiveql;
            log.info(result);
            log.error(e.getMessage());
            throw new Exception(e);
        }
    }



    /**
     * 获取指定表名的表字段信息
     * @param tableName
     * @return List<String>
     * @throws Exception
     */
    public List<Map<String,String>> getTableInfo(String tableName) throws Exception {
        List<Map<String,String>> resultList = new ArrayList<Map<String,String>>();
        Connection con = getConn();
        String result = "";
        ResultSet res1 = con.createStatement().executeQuery("show columns from "+tableName);
        while(res1.next()) {
            String columnName = res1.getString("Column");
            String columnType = res1.getString("Type");
            Map<String,String> columnMap = new HashMap<String,String>();
            columnMap.put(columnName,columnType);
            resultList.add(columnMap);
        }

        return resultList;
    }
    /**
     * 获取jdbc连接
     * @return Connection
     */
    public static Connection getConn(){
        long start = System.currentTimeMillis();
        Connection con = null;
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
/*            if (ds == null) {
                Map<String, String> conf = new HashedMap();
                conf.put(DruidDataSourceFactory.PROP_DRIVERCLASSNAME, getProperties("presto.driverName"));
                conf.put(DruidDataSourceFactory.PROP_URL, getProperties("presto.connection.url"));
                conf.put(DruidDataSourceFactory.PROP_USERNAME, getProperties("remote.username"));
                conf.put(DruidDataSourceFactory.PROP_PASSWORD, getProperties("remote.password"));
                conf.put(DruidDataSourceFactory.PROP_INITIALSIZE, "8");
                conf.put(DruidDataSourceFactory.PROP_MAXACTIVE,"30");
                conf.put(DruidDataSourceFactory.PROP_MAXWAIT,"2000");
                conf.put(DruidDataSourceFactory.PROP_TIMEBETWEENEVICTIONRUNSMILLIS,"60000");
                conf.put(DruidDataSourceFactory.PROP_MINEVICTABLEIDLETIMEMILLIS,"300000");
                conf.put(DruidDataSourceFactory.PROP_REMOVEABANDONED,"true");
                conf.put(DruidDataSourceFactory.PROP_REMOVEABANDONEDTIMEOUT,"1800");
                conf.put(DruidDataSourceFactory.PROP_LOGABANDONED,"true");
                DruidDataSource druidDS = (DruidDataSource) DruidDataSourceFactory.createDataSource(conf);
                druidDS.setBreakAfterAcquireFailure(true);
                druidDS.setConnectionErrorRetryAttempts(5);
                ds = druidDS ;
            }
            con = ds.getConnection() ;*/
            Class.forName( getProperties("presto.driverName"));
            con = DriverManager.getConnection(getProperties("presto.connection.url"),getProperties("remote.username"),getProperties("remote.password"));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            log.error(e.getMessage());
        }
        log.info("PrestoClient.getConn cost time : "+(System.currentTimeMillis()-start)/1000.0 +" seconds " );
        return con;
    }



    /**
     * 获取相关配置
     * @param key
     * @return
     */
    public static String getProperties(String key){
        String value = "";
        Properties prop = new Properties();
        InputStream in = PrestoClient.class.getResourceAsStream("/hiveConfiguration.properties");
        try {
            prop.load(in);
            value = prop.getProperty(key).trim();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }

    public int getTotalCount(String hivesql){
        int result = 0 ;
        try{
            Connection conn = getConn();
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(hivesql);
            boolean hasHeaders = false ;
            while(res.next()) {
                return res.getInt(1);
            }
        }catch (Exception e){
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return result ;
    }
}

