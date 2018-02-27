package com.stock.analysis.hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class Main {
	
//	String[]fimaly={"http","user"};
	private static String TABLENAME="time_series_data";
	private static String[]COLUMN_FAMALY={"info"};
	private static String COL1="v1";
	private static String COL2="v2";
	private static String COL3="v3";
	
	private static String FILEPATH="/home/zqr/桌面/tradingDate.csv";
	
	
 public static void main(String[]args){
	 
	 HbaseOpInterface Himp=new HbaseOperationTable();
	 
	 
		
		try {
			/*
			 * 创建一个表
			 */
			//Himp.createTable(TABLENAME, COLUMN_FAMALY);
			/*
			 * 删除一个表
			 */
			//Himp.dropTable("newtable");
			/*
			 * 插入数据
			 */
			ReadCsv rc=new ReadCsv();
			List<String>stockdata=rc.readcsv(FILEPATH);
			String uuid="";
			for(String vaule:stockdata){
				//随机生成uuid
				for(int i=0;i<10;i++){
				    uuid = UUID.randomUUID().toString().replaceAll("-", "");
					System.out.println(uuid);
					}
				data dt=new data();
				String[]splitarr=vaule.split(",");
				dt.setStock_code(splitarr[0]);
				dt.setItem_vaule1(Double.parseDouble(splitarr[1]));
				dt.setItem_vaule2(Double.parseDouble(splitarr[2]));
				dt.setItem_vaule2(Double.parseDouble(splitarr[3]));
				//rowkey(item_id,trading_date,stock_code)
				//获取系统当前时间，其实这里可以不存，因为hbase每存入一条数据就会自己存一条时间戳
				Date day=new Date();    
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
				String dfs=df.format(day);
				String rowkey=uuid+dfs+dt.getStock_code().toString();
				//插入stock_code
				Himp.putData(TABLENAME, rowkey, "info",COL1, String.valueOf(dt.getStock_code()));
				//插入第一列v1
				Himp.putData(TABLENAME, rowkey, "info",COL1, String.valueOf(dt.getItem_vaule1()));
				//插入第二列v2
				Himp.putData(TABLENAME, rowkey, "info",COL2, String.valueOf(dt.getItem_vaule2()));
				//插入第三列v3
				Himp.putData(TABLENAME, rowkey, "info",COL3, String.valueOf(dt.getItem_vaule3()));
			}
//			int m=1;
//			while(m<10000){
//				String str=String.valueOf(m);
//			Himp.putData("t", str, "f","c", str);
//			m++;
//			System.out.println("插入第"+m+"数据成功");
//			}
			/*
			 * get获取数据
			 */
			//Himp.getValueBySeries("book", "3", "b", "0001");
			//Himp.getValueBySeries("user", "0001", "u", "3");
			
			/*
			 * 根据rowkey的范围查找数据
			 */
			//Himp.scanByStartAndStopRow("data", "95001", "95003");
			/*
			 * 获取表中所有数据
			 */
			//Himp.scanVersions("data", 3);
			//Himp.getValueByTable("book");
			/*
			 * 按照起止时间戳查询
			 */
			//Himp.scanByStartAndStopRow("data", "95001", "95003");
			
			//Himp.splicpolicy("student");
			
			 /*
			  * +++++++++++++++++++++++++++
			  * +        IS BEGIN!	      +
			  * +++++++++++++++++++++++++++
			  */
	 
 }	 
    catch (IOException e) {
		e.printStackTrace();
	}
 }
}
