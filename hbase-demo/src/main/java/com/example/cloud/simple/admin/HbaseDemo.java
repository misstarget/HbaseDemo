package com.example.cloud.simple.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;


public class HbaseDemo {

	public static Logger logger = Logger.getLogger(HbaseTest.class.getName()); 
	
	private Configuration conf = null;
	
	private Connection connection = null;
	
	
	public HbaseDemo() {
		conf = HBaseConfiguration.create();
		conf.set("hadoop.home.dir", "/usr/hdp/2.5.3.0-37/hadoop");
		conf.set("hbase.rootdir", "hdfs://www.hadoop2.org/apps/hbase/data");
		conf.set("hbase.zookeeper.quorum", "www.hadoop2.org,www.hadoop3.org,www.hadoop4.org");
//		conf.set("hbase.zookeeper.quorum", "172.16.3.5,172.16.3.6,172.16.3.7");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	 /** 
     * 查询全部 
     */  
    public void queryAll() {  
        Scan scan = new Scan();  
        try {  
        	Table table = connection.getTable(TableName.valueOf("AgentStat"));
        	/*Get get = new Get("securitycustom".getBytes());
        	Result result = table.get(get);
        	List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {

                byte[] familyArray = cell.getFamilyArray();
                byte[] qualifierArray = cell.getQualifierArray();
                byte[] valueArray = cell.getValueArray();

                System.out.println("row value is:" + new String(familyArray) + new String(qualifierArray) 
                                                                             + new String(valueArray));
            }*/
            ResultScanner results = table.getScanner(scan);  
            for (Result result : results) {  
                int i = 0;  
                for (Cell rowKV : result.listCells()) {  
                    if (i++ == 0) {  
                    	//logger.info("rowkey:" + new String(rowKV.getRowArray()) + " ");
                    	byte[] bytes = rowKV.getRowArray();
                    	List<Byte> list = new ArrayList<>();
                        for (byte b : bytes) {
							
						}
                    }  
                   // logger.info("qualifier:" + new String(rowKV.getQualifierArray()) + " ");
                   // System.out.print(" " + new String(rowKV.getQualifierArray(), "GBK") + " ");  
                    //logger.info("value:" + new String(rowKV.getValueArray()));
                    //System.out.print(":" + new String(rowKV.getValueArray(), "GBK"));  
                }  
  
  
                System.out.println();  
            }
        } catch (IOException e) {  
            logger.error(e.getMessage());  
        }  
        finally {
        	try {
    			logger.debug("testAfter-------");
    			connection.close();
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
        }
    }  
	
    
    public static void main(String[] args) throws Exception {  
    	HbaseDemo hb = new HbaseDemo();
    	hb.queryAll();		
    }  
}
