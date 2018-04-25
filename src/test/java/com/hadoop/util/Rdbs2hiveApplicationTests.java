package com.hadoop.util;

import com.hadoop.util.common.DBDataImport;
import com.hadoop.util.common.dbUtil.DBUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class Rdbs2hiveApplicationTests {


	@Test
	public void dataImportFromMysql() throws InterruptedException {
		String user = "root";
		String password = "123456";
		String host = "localhost";
		int port = 3306 ;
		String databaseName = "talebase_smartdata";
		String tablename = "sys_user" ;
		int dataSourceType = 1;
        /*        String user = "sa";
                String password = "123456xX";
                String url = "jdbc:sqlserver://192.168.1.68:1433;DatabaseName=Assessment_4_2_0";
                String tablename = "Customer" ;
                int dataSourceType = 2;*/
		String url = DBUtil.genDatabaseUrl(host,port,databaseName,dataSourceType);
		DBDataImport dataImport = new DBDataImport(user,password,url,dataSourceType,tablename,"e:/upload/");
		dataImport.start();
		dataImport.join();
	}
	@Test
	public void dataImportFromSqlServer() throws InterruptedException {

		String host = "192.168.1.68";
		int port = 1433 ;
		String databaseName = "Assessment_4_2_0";
		String user = "sa";
		String password = "123456xX";
		String tablename = "Customer" ;
		int dataSourceType = 2;
		String url = DBUtil.genDatabaseUrl(host,port,databaseName,dataSourceType);
		DBDataImport dataImport = new DBDataImport(user,password,url,dataSourceType,tablename,"e:/upload/");
		dataImport.start();
		dataImport.join();
	}
}
