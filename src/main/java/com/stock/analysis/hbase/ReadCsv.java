package com.stock.analysis.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadCsv {
    //读取csv
	@SuppressWarnings("resource")
	public static List<String> readcsv(String path){

		List<String> allString=null;
		// CSV文件路径
	    File csv = new File(path);  
	    BufferedReader br = null;
	    try
	    {
	        br = new BufferedReader(new FileReader(csv));
	    
	    } catch (FileNotFoundException e)
	    {
	        e.printStackTrace();
	    }
	    String line = "";
	    String everyLine = "";
	    try {
	            allString = new ArrayList<>();
	          //读取到的内容给line变量
	            while ((line = br.readLine()) != null)  
	            {
	                everyLine = line;
	                System.out.println(everyLine);
	                allString.add(everyLine);
	            }
//	            System.out.println("csv表格中所有行数："+allString.size());
//              result:
//	            00001,0.01,1.02,-1.03
//	            00002,0.03,0.22,0.01
//	            00003,0.04,1.24,0.05
//	            00004,1.04,0.12,-0.02
//	            csv表格中所有行数：4
	    } catch (IOException e)
	    {
	        e.printStackTrace();
	    }
       return allString;
	}
}
