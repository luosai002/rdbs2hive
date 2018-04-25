# rdbs2hive
关系型数据库导入到hive 目前支持mysql ,sqlserver,oracle    

为了把关系型数据库表导入hive,写了一个工具类。
具体思路：
1、读取关系型数据库表信息。    
2、根据表信息创建hive表    
3、读取表数据    
4、写入文件  
5、上传文件到hive,服务器，本地加载到hive中。  
使用方法：  

1、在配置文件hiveConfiguration.properties 配置好相关信息，如下：    

```
hive.driverName=org.apache.hive.jdbc.HiveDriver
hive.connection.url=jdbc:hive2://192.168.2.39:10000/default
remote.ip=192.168.2.39
remote.username=root
remote.password=123456xX
remote.filepath=/root

```
remote.ip 是hive所在的服务器ip地址  
remote.username 登录hive服务用户名  
remote.password 登录hive服务密码  
remote.filepath 本地文件上传到hive服务器所在文件夹  

2、配置需要导入的关系型数据库信息，如下是测试类：      

```
	@Test
	public void dataImportFromMysql() throws InterruptedException {
		String user = "root";
		String password = "123456";
		String host = "localhost";
		int port = 3306 ;
		String databaseName = "talebase_smartdata";
		String tablename = "sys_user" ;
		int dataSourceType = DataSourceType.MYSQL_TYPE;
		String url = DBUtil.genDatabaseUrl(host,port,databaseName,dataSourceType);
		DBDataImport dataImport = new DBDataImport(user,password,url,dataSourceType,tablename,"e:/upload/");
		dataImport.start();
		dataImport.join();
	}
```
