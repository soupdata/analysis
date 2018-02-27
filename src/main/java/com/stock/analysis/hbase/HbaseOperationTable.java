package com.stock.analysis.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseOperationTable implements HbaseOpInterface {
	//=======================================================================
	   //=========================connecation and congriguration img============
		/*
		 * 连接池配置
		 */
		public static Configuration configuration;
		/*
		 * 连接池对象
		 */
	    public static Connection connection;
	    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
		private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
	    /*
		 * Admin
		 */
	    public static Admin admin;
	    /**
		 * HBase位置
		 */
		@SuppressWarnings("unused")
		private static final String HBASE_POS = "localhost";

		/**
		 * ZooKeeper位置
		 */
		private static final String ZK_POS = "localhost";

		/**
		 * zookeeper服务端口
		 */
		private final static String ZK_PORT_VALUE = "2181";
	    /*
	     * 建立连接
	     */
	   //==================================================================
	   //=====================contry open and close========================
	    public static void init(){
	        configuration  = HBaseConfiguration.create();
	        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
	        configuration.set(ZK_QUORUM, ZK_POS);
			configuration.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
	        try{
	            connection = ConnectionFactory.createConnection(configuration);
	            admin = connection.getAdmin();
	        }catch (IOException e){
	            e.printStackTrace();
	        }
	    }
	    /*
	     * 关闭连接
	     */
	    public static void close(){
	        try{
	            if(admin != null){
	                admin.close();
	            }
	            if(null != connection){
	                connection.close();
	            }
	        }catch (IOException e){
	            e.printStackTrace();
	        }
	    }
	  //=======================================================================
	  //===============================database operation======================
	    /**
	     * 创建一个表
	     * @param tableName 表名
	     * @param faimly    列族：String      
		 * @throws IOException 
	     */
		@Override
		public boolean createTable(String tableName, String[]faimly) throws IOException {
			init();
			//此标志为判断表是否创建成功
			boolean IsSuccess=false;
			//调用hbase内部接口访问数据库,获取数据库中所有的表名
			TableName table=TableName.valueOf(tableName);
			//判断表是否存在于数据库中，否则开始创建
			if(!admin.tableExists(table)){
				System.out.println(tableName + "开始建表..."); 
				//调用HTableDescriptor在数据库中建表
				HTableDescriptor descriptor = new HTableDescriptor(table);
				 int len = faimly.length;  
	             for (int index = 0; index < len; index++) {  
	            	 //调用 HColumnDescriptor在表中创建列族
	                 HColumnDescriptor family = new HColumnDescriptor(  
	                		 faimly[index]);  
	                 descriptor.addFamily(family);  
	             }  
	             //实际创建
	             admin.createTable(descriptor);  
	             System.out.println(tableName + " 表创建成功！");
	             IsSuccess=true;
			}
			close();
			return IsSuccess;
			
		}
		/**
		 * 删除一个表
		 * @param tableName 　表名
		 * @throws IOException 
		 * */
		@Override
		public boolean dropTable(String tableName) throws IOException {
			        init();
			        //此标志为判断表是否删除成功
					boolean IsSuccess=false;
					//调用hbase内部接口访问数据库,获取数据库中所有的表名
					TableName table=TableName.valueOf(tableName);
					//判断表是否存在于数据库中，有则执行删除操作
					if(admin.tableExists(table)){
						admin.disableTable(table);  
		                admin.deleteTable(table);  
		                IsSuccess=true;
		                System.out.println(tableName + " 表删除成功！");
					}else{
						System.out.println(tableName + " 抱歉，该数据库中不存在该表！");
					}
			close();
			return IsSuccess;
		}

		/**
		 * 向指定表中插入数据
		 * 
		 * @param tableName
		 *            要插入数据的表名
		 * @param rowkey
		 *            指定要插入数据的表的行键
		 * @param family
		 *            指定要插入数据的表的列族family
		 * @param qualifier
		 *            要插入数据的qualifier,限定符
		 * @param value
		 *            要插入数据的值value,object类型
		 * */
		@Override
		public void putData(String tableName, String rowkey, String family,
				String qualifier, String value) throws IOException {
			init();
			TableName tablename = TableName.valueOf(tableName);
			//判断表是否存在于数据库中，是则写入数据
					if(admin.tableExists(tablename)){
						try (
								//从数据库中获取表，其实 也就是打开外部与数据库的通道，从而操作表
								Table table = connection.getTable(TableName.valueOf(tableName
								.getBytes()))) 
							{
							    //创建put对象
								Put put = new Put(rowkey.getBytes());
								put.addColumn(family.getBytes(), qualifier.getBytes(),
										value.toString().getBytes());
								//System.out.println("开始插入数据！");
								table.put(put);
								//System.out.println("插入数据成功！");
								table.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						System.out.println("插入数据的表不存在，请指定正确的表名! ");
					}
					
					close();
			
		}
		/**
		 * 根据指定表获取指定行键rowkey和列族family的数据 并以字符串的形式返回查询到的value和时间戳
		 * 
		 * @param tableName
		 *            要获取表 tableName 的表名
		 * @param rowKey
		 *            指定要获取数据的行键
		 * @param family
		 *            指定要获取数据的列族元素
		 * @param qualifier
		 *            指定要获取数据的qualifier
		 *            
		 * */
		@Override
		public String getValueBySeries(String tableName, String rowKey,
				String family, String qualifier) throws IllegalArgumentException,
				IOException {
			init();
			Table table = null;
			String resultStr = null;
			try {
				table = connection
						.getTable(TableName.valueOf(tableName.getBytes()));
				Get get = new Get(Bytes.toBytes(rowKey));
				get.setMaxVersions(3);
				get.setMaxVersions();
				get.getMaxVersions();
			
				get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
				Result res = table.get(get);
//				//=====================================
//				List<Cell> cells = res.listCells();  
//	            String re = "";  
//	            List<String> list = new ArrayList<String>();  
//	            if(null != cells && !cells.isEmpty()){  
//	                for(Cell ce:cells){  
//	                    re = Bytes.toString(ce.getValueArray(),  
//	                            ce.getValueOffset(),  
//	                            ce.getValueLength());  
//	                    System.out.println("re:"+res+" timestamp:"+ce.getTimestamp());  
//	                    list.add(re);  
//	                }  
//	            }  
//	         
//	            System.out.println("result:"+list);  


	            //===========================================

				//======================================================================
				System.out.println("第一种返回方式："+res);
				//获取value
				byte[] result = res.getValue(Bytes.toBytes(family),
						Bytes.toBytes(qualifier));
				resultStr = Bytes.toString(result);
				//获取时间戳
				List<Cell> cs=res.listCells();
		        for(Cell cell:cs){
				long timestamp=cell.getTimestamp();
				System.out.println("第二种返回方式:"+family+":"+qualifier+"        "
				                 +"TimeStamp"+"="+timestamp+"  value="+resultStr);
		        }
			} finally {
				IOUtils.closeQuietly(table);
			}
			return resultStr;
		}
		/**
		 * 根据指定表获取指定行键rowKey和列族family的数据 并以Map集合的形式返回查询到的结果
		 * 
		 * @param tableName
		 *            要获取表 tableName 的表名
		 * @param rowKey
		 *            指定的行键rowKey
		 * @param family
		 *            指定列族family
		 * */
		
		@Override
		public Map<String, String> getAllValue(String tableName, String rowKey,
				String family) throws IllegalArgumentException, IOException {
			
			return null;
		}
		/**
		 * 根据主键范围查询
		 * @param tableName 表名
		 * @param startRow 开始的行健
		 * @param stopRow  结束的行健
		 * **/
		@Override
		public void scanByStartAndStopRow(String tableName, String startRow,
				String stopRow) throws Exception {
			init();
			Table table= connection.getTable(TableName.valueOf(tableName));
			Scan scan=new Scan();
			scan.setStartRow(Bytes.toBytes(startRow));
			scan.setStopRow(Bytes.toBytes(stopRow));
			ResultScanner rs=table.getScanner(scan);
			for(Result r:rs){
				for(Cell cell:r.rawCells()){
					System.out.print(" Rowkey: "+new String(CellUtil.cloneRow(cell)));
					System.out.print(" column: "+new String(CellUtil.cloneFamily(cell)));
					System.out.print(" :qualifier: "+new String(CellUtil.cloneQualifier(cell)));
					System.out.print(" value: "+new String(CellUtil.cloneValue(cell)));
					System.out.println(" timestamp: "+cell.getTimestamp());
						
				}
			}
			table.close();//释放资源
			
		}
		/**
		 * @param tableName 表名
		 * 查询表中的所有数据 无返回值，直接在控制台打印结果
		 * */
		@SuppressWarnings("deprecation")
		public void getValueByTable(String tableName) throws Exception {
			init();
			Table table = null;
			try {
				table = connection.getTable(TableName.valueOf(tableName));
				Scan scan=new Scan();
//				scan.setMaxVersions(3);
				ResultScanner rs = table.getScanner(scan);
				for (Result r : rs) {
					System.out.println("row:" + new String(r.getRow()));
					for (KeyValue keyValue : r.raw()) {
						System.out.println("               column=" + new String(keyValue.getFamily())
								+ ":" + new String(keyValue.getQualifier())
								+ "   value:" + new String(keyValue.getValue()));
					}
				}
			} finally {
				IOUtils.closeQuietly(table);
			}
			table.close();//释放资源
		}
		/**
		 * 删除列族
		 * @param tableName 　表名
		 * @param family  需要删除的列族(此处family类型可改为集合或数组)
		 */
		@Override
		public boolean deleteColumn(String tableName, String family)
				throws IOException {
			init();
			boolean IsSuccess=false;
			TableName tablename = TableName.valueOf(tableName);
			byte[]familyB={Byte.valueOf(family)};
			if(admin.tableExists(tablename)){
				System.out.println("开始删除"+tableName+"表的"+family+"列族");
				admin.deleteColumn(tablename, familyB);
				System.out.println("删除成功！");
				IsSuccess=true;
			}
			return IsSuccess;
		}
		/**
		 * 删除一行(原则上一行一行删除太慢了，直接删表)
		 * @param tableName 　表名
		 * @param rowkey  需要删除的一行
		 */
		@Override
		public boolean deleteRowkey(String tableName, String rowkey)
				throws IOException {
			init();
			boolean IsSuccess=false;
			TableName tablename = TableName.valueOf(tableName);
			Table table = connection.getTable(tablename);
			if(admin.tableExists(tablename)){
				Delete delete=new Delete(rowkey.getBytes());
				System.out.println("开始删除"+tableName+"表的,rowkey为："+rowkey+"的行");
				table.delete(delete);
				System.out.println("删除成功！");
				IsSuccess=true;
			}
			table.close();//释放资源
			return IsSuccess;
			
		}
		/**
	     *删除qualifier
	     *@param tableName 　表名
	     *@param rowkey  需要删除的一行
	     *@param family  需要删除的列族        
	     *@param qualifier 需要删除的一类限定副
	     */
		@Override
		public boolean deleteQualifier(String tableName, String rowkey,
				String fimaly, String qualifier) throws IOException {
			init();
			boolean IsSuccess=false;
			TableName tablename = TableName.valueOf(tableName);
			Table table = connection.getTable(tablename);
			//String类型转换成Byte[]
			byte[]familyB={Byte.valueOf(fimaly)};
			byte[]qualifierB={Byte.valueOf(qualifier)};
			if(admin.tableExists(tablename)){
				Delete delete=new Delete(rowkey.getBytes());
				System.out.println("开始删除"+tableName+"表的,列族："+fimaly+"的"+qualifier+"列");
				delete.addColumn(familyB, qualifierB);
				table.delete(delete);
				System.out.println("删除成功！");
				IsSuccess=true;
			}
			table.close();//释放资源
			return IsSuccess;
		}
		@Override
		public void scanVersions(String tableName, int versions) throws Exception {
			init();
			Table table=  connection.getTable(TableName.valueOf(tableName));
			Scan scan=new Scan();
			scan.setMaxVersions(versions);
			scan.setMaxVersions();
			
			long strnum=1504408755487L;
			long endnum=1504408755488L;
			scan.setTimeRange(strnum,endnum);
			//scan.getTimeRange();
			System.out.println("banbenshu::::::"+scan.setMaxVersions());
			
			table.close();
			
		}
		/*
		 * (non-Javadoc)
		 * @see com.hadoop.hbase.operationinterface.HbaseOperationInterface#splicpolicy(java.lang.String)
		 * 分区策略
		 * 
		    IncreasingToUpperBoundRegionSplitPolicy，0.94.0默认region split策略。根据公式min(r^2*flushSize，maxFileSize)确定split的maxFileSize，其中r为在线region个数，maxFileSize由hbase.hregion.max.filesize指定。
		    ConstantSizeRegionSplitPolicy，仅仅当region大小超过常量值（hbase.hregion.max.filesize大小）时，才进行拆分。
		    DelimitedKeyPrefixRegionSplitPolicy，保证以分隔符前面的前缀为splitPoint，保证相同RowKey前缀的数据在一个Region中
		    KeyPrefixRegionSplitPolicy，保证具有相同前缀的row在一个region中（要求设计中前缀具有同样长度）。指定rowkey前缀位数划分region，通过读取table的prefix_split_key_policy.prefix_length属性，该属性为数字类型，表示前缀长度，在进行split时，按此长度对splitPoint进行截取。此种策略比较适合固定前缀的rowkey。当table中没有设置该属性，或其属性不为Integer类型时，指定此策略效果等同与使用IncreasingToUpperBoundRegionSplitPolicy。
	        PS:3,4用的比较少
		 */
//		@Override
//		public void splicpolicy(String tablename) throws IOException {
//			init();
//			 TableName tn = TableName.valueOf(tablename);
//			 Table table = connection.getTable(tn);
//			 //判断条件,表名是否存在
//			 if(admin.tableExists(tn)){
//			         HTableDescriptor htd = table.getTableDescriptor();  
//			         //new一个新的对象
//			         HTableDescriptor newHtd = new HTableDescriptor(htd);  
//				     newHtd.setValue(HTableDescriptor. SPLIT_POLICY, KeyPrefixRegionSplitPolicy.class .getName());// 指定策略  
//				     newHtd.setValue("prefix_split_key_policy.prefix_length", "2");
//				     //5M
//				     newHtd.setValue("MEMSTORE_FLUSHSIZE", "5242880");
//				     admin.disableTable(tn);  
//				     admin.modifyTable(tn, newHtd);  
//				     admin.enableTable( tn);  
//			 }
//			
//		}
	@Override
	public void splicpolicy(String tablename) throws IOException {
		// TODO Auto-generated method stub
		
	}
}

