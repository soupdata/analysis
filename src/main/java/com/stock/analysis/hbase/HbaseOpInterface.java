package com.stock.analysis.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public interface HbaseOpInterface {
	/**
	 * @param tableName
	 *            创建一个表 tableName 指定的表名　 seriesStr
	 * @param seriesStr
	 *            以字符串的形式指定表的列族，每个列族以逗号的形式隔开,(例如：＂f1,f2＂两个列族，分别为f1和f2)
	 * @throws IOException 
	 **/
	boolean createTable(String tableName, String[]faimly) throws IOException;

	/**
	 * 删除指定表名的表
	 * @param tableName 　表名
	 * @throws IOException 
	 * */
	boolean dropTable(String tableName) throws IOException;
	/**
	 * 删除列族
	 * @param tableName 　表名
	 * @param family  需要删除的列族
	 */
	boolean deleteColumn(String tableName,String family) throws IOException;
	/**
	 * 删除一行(原则上一行一行删除太慢了，直接删表)
	 * @param tableName 　表名
	 * @param rowkey  需要删除的一行
	 */
	boolean deleteRowkey(String tableName,String rowkey) throws IOException;
	/**
     *删除qualifier
     *@param tableName 　表名
     *@param rowkey  需要删除的一行
     *@param family  需要删除的列族        
     *@param qualifier 需要删除的一类限定副
     */
	boolean deleteQualifier(String tableName,String rowkey,String fimaly,String qualifier) throws IOException;
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
	 *            要插入数据的qualifier
	 * @param value
	 *            要插入数据的值value
	 * */
    void putData(String tableName, String rowkey, String family,
			String qualifier, String value) throws IOException;
   
  
	/**
	 * 根据指定表获取指定行键rowkey和列族family的数据 并以字符串的形式返回查询到的结果
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
     String getValueBySeries(String tableName, String rowKey,
			String family,String qualifier) throws IllegalArgumentException, IOException ;
   
  //================================待添加方法===========================================
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
     
    Map<String, String> getAllValue(String tableName,
			String rowKey, String family) throws IllegalArgumentException, IOException ;
    
  //====================================================================================	   
    /**
	 * 根据rowkey扫描一段范围
	 * @param tableName 表名
	 * @param startRow 开始的行健
	 * @param stopRow  结束的行健
	 * **/
    void scanByStartAndStopRow(String tableName,String startRow,String stopRow)throws Exception;
 //================================================待添加方法=========================================
    /**
	 * 根据标准时间查询一个月的数据
	 * @param tableName 表名
	 * @param startRow 开始的行健
	 * @param stopRow  结束的行健
     * @throws Exception 
	 * **/
//    void scanMonth(String tableName,String startRow,String stopRow )throws Exception;
   // 根据table查询表中的所有数据 无返回值，直接在控制台打印结果
    public void getValueByTable(String tableName) throws Exception;
//====================================================================================================   
   
    /**
	 * 根据rowkey扫描一段范围
	 * @param tableName 表名
	 * @param versions 显示的版本号
	 * **/
    void scanVersions(String tableName,int versions)throws Exception; 
    
    /*
     * 更新现有表的split策略
     */
    void splicpolicy(String tablename) throws IOException;
    
}
