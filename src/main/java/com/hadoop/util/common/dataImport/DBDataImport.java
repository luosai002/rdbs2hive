package com.hadoop.util.common.dataImport;

import com.hadoop.util.common.UUIDUtil;
import com.hadoop.util.common.dbUtil.ConnUtil;
import com.hadoop.util.common.dbUtil.DBUtil;
import com.hadoop.util.common.dbUtil.DataSourceType;
import com.hadoop.util.common.enums.ImportOperatorType;
import com.hadoop.util.common.fileUtil.FileWrite;
import com.hadoop.util.hive.HiveOperate;
import com.hadoop.util.model.ColumnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

/**
 * Created by sai.luo on 2017-5-19.
 */
public class DBDataImport extends  Thread {

    private static final Logger log = LoggerFactory.getLogger(DBDataImport.class) ;
    private FileWrite fileWrite = new FileWrite();
    private static final String split = "\t";

    private String username ;
    private String password;
    private String url ;
    private int dataSourceType ;
    private String sourceTableName;
    private int operateType ;
    private String localTempPath ;
    /**
     *
     * @param username 用户名
     * @param password 密码
     * @param url 数据库路径
     * @param dataSourceType 数据库类型
     * @param tableName 数据库表名
     * @param operateType 操作类型
     */
    public DBDataImport(String username, String password, String url, int dataSourceType, String tableName,int operateType,String localTempPath) {
        this.username = username;
        this.password = password;
        this.url = url;
        this.dataSourceType = dataSourceType;
        this.sourceTableName = tableName;
        this.operateType = operateType;
        this.localTempPath = localTempPath ;
    }

    public static void main(String[] args) {
            String user = "root";
            String password = "123456";
            String host = "localhost";
            int port = 3306 ;
            String databaseName = "talebase_smartdata";
            String tablename = "sys_report_dedicated_respondent_dept" ;
            int dataSourceType = 1;
        /*        String user = "sa";
                String password = "123456xX";
                String url = "jdbc:sqlserver://192.168.1.68:1433;DatabaseName=Assessment_4_2_0";
                String tablename = "Customer" ;
                int dataSourceType = 2;*/
            String url = DBUtil.genDatabaseUrl(host,port,databaseName,dataSourceType);
            DBDataImport dataImport = new DBDataImport(user,password,url,dataSourceType,tablename, ImportOperatorType.ADD_TYPE.getOperateType(),"e:/upload/");
            dataImport.start();


    }
    @Override
    public void run() {

        try {
            //1、获取数据库连接
            Connection conn = ConnUtil.getConn(DataSourceType.getDriverName(dataSourceType),url,username,password);
            //2、获取表的信息
            List<ColumnInfo> columnMapList = DBUtil.getTableColumnInfo(sourceTableName, conn);
            //3、转换为hive表的对应类型
            DBUtil.tableColumnInfoToHiveColnmnInfo(columnMapList,dataSourceType);
            columnMapList.forEach(columnInfo -> log.info(columnInfo.toString()));
            //4、获取表的所有数据
            List<List<Object>> resultList = DBUtil.getTableDatas(sourceTableName, conn);
            int dataCount = resultList.size();
            //5、写入文件 并导入到hive
            if (dataCount > 0) {
                String tmpFile = sourceTableName;
                fileWrite.writeMapList2File(resultList,  localTempPath+ sourceTableName,split);
                HiveOperate.executeHiveOperate(sourceTableName,null,tmpFile,columnMapList,null,split,operateType,localTempPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

    }
}
