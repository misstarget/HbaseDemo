package com.example.cloud.simple.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * 
 */

/**
 * @author xieanyue
 *
 */
public class HbaseTest{
	public static Logger logger = Logger.getLogger(HbaseTest.class.getName()); 
	
	private Configuration conf = null;
	
	private Connection connection = null;
	
	
	@Before
	public void testBefore(){
		logger.debug("init: create conf");
		conf = HBaseConfiguration.create();
		conf.set("hadoop.home.dir", "/usr/hdp/2.5.3.0-37/hadoop");
		conf.set("hbase.rootdir", "hdfs://www.hadoop2.org/apps/hbase/data");
		conf.set("hbase.zookeeper.quorum", "www.hadoop2.org,www.hadoop3.org,www.hadoop4.org");
//		conf.set("hbase.zookeeper.quorum", "172.16.3.5,172.16.3.6,172.16.3.7");
		logger.debug("conf:"+conf);
		logger.debug("conf:"+conf.get("hbase.rootdir"));
		logger.debug("conf:"+conf.get("hbase.zookeeper.quorum"));
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testCellGet() throws Exception{
//		HTable table = new HTable(conf, "user");
		String tableName = "Traces";
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes("00001"));
		get.addColumn(Bytes.toBytes("A"), Bytes.toBytes("S"));
		get.setMaxVersions(5);
		Result result = table.get(get);
		byte[] val = result.getValue(Bytes.toBytes("A"), Bytes.toBytes("S"));
		logger.debug("val:"+Bytes.toString(val));
		table.close();
	}
	
	@Test
	public void testGetResult() throws Exception{
//		HTable table = new HTable(conf, "user");
		String tableName = "Traces";
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes("5184000"));
		get.setMaxVersions(5);
		Result result = table.get(get);
		
		List<Cell> cells = result.listCells();
		for(Cell cell:cells){
            String rowkey=Bytes.toString(CellUtil.cloneRow(cell));//取到行键
            long timestamp=cell.getTimestamp();//取时间戳
            String fname=Bytes.toString(CellUtil.cloneFamily(cell));//取到列族名
            String qualifier=Bytes.toString(CellUtil.cloneQualifier(cell));//取修饰名，即列名
            String value=Bytes.toString(CellUtil.cloneValue(cell)); //取值
            logger.debug("rowkey=="+rowkey+"---timestamp=="+timestamp+"---qualifier=="+fname+"=>"+qualifier+"---value=="+value);
        }
		
//		//遍历出result中所有的键值对
//		for(KeyValue kv : result.list()){
//			String family = new String(kv.getFamily());
//			logger.debug("family:"+family);
//			String qualifier = new String(kv.getQualifier());
//			logger.debug("qualifier:"+qualifier);
//			logger.debug("value:"+new String(kv.getValue()));
//			
//		}
		
		table.close();
	}
	
	@Test
	public void testAddRecord(){		
		String tableName = "Traces";
		String rowKey = "5184000";
		String family = "info";
		String qualifier = "address";
		String value = "chengdu";
		try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            logger.debug("table:"+table);
            table.put(put);
            table.close();
            logger.debug("insert recored " + rowKey + " to table " + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	/**
	 * 删除一行记录
	 */
	@Test
	public void testDelRecord() throws IOException {
		String tableName = "user";
		String rowKey = "rk0001";
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        del.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"));
        list.add(del);
        table.delete(list);
        logger.debug("del recored " + rowKey + " ok.");
        table.close();
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testTable(){
		String tableName = "AgentEvent";
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			HTableDescriptor tableDescriptor = table.getTableDescriptor();
			HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
			for(HColumnDescriptor columnFamily:columnFamilies){
				logger.debug(columnFamily.getNameAsString());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}
	
	/**
	 * 省略
	 */
	@Test
	public void testHBaseAdmin(){
		try {
			String tableName = "user";
			Admin admin = connection.getAdmin();
			HTableDescriptor htabledescriptor = null;
			admin.createTable(htabledescriptor);
//			admin.disableTable(TableName.valueOf(tableName));
//			admin.deleteTable(TableName.valueOf(tableName));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}
	
	 /** 
     * 查询全部 
     */  
	@Test
    public void queryAll() {  
        Scan scan = new Scan();  
        try {  
        	Table table = connection.getTable(TableName.valueOf("user"));
            ResultScanner results = table.getScanner(scan);  
            Iterator<Result> iterator = results.iterator();
            while (iterator.hasNext()) {
				Result result = (Result) iterator.next();
				int i = 0;  
                for (Cell rowKV : result.listCells()) {  
                    if (i++ == 0) {  
                        System.out.print("rowkey:" + new String(rowKV.getRowArray()) + " ");  
                    }  
                    System.out.print(" " + new String(rowKV.getQualifierArray()) + " ");  
                    System.out.print(":" + new String(rowKV.getValueArray()));  
                }  
  
  
                System.out.println();
			}
            /*for (Result result : results) {  
                int i = 0;  
                for (Cell rowKV : result.listCells()) {  
                    if (i++ == 0) {  
                        System.out.print("rowkey:" + new String(rowKV.getRowArray()) + " ");  
                    }  
                    System.out.print(" " + new String(rowKV.getQualifierArray()) + " ");  
                    System.out.print(":" + new String(rowKV.getValueArray()));  
                }  
  
  
                System.out.println();  
            }  */
        } catch (IOException e) {  
        	logger.error(e.getMessage());  
        }  
  
  
    }  
	
	
	 @Test
     public void queryTable() throws IOException{

        System.out.println("---------------查询整表数据 START-----------------");

        // 取得数据表对象
        Table table = connection.getTable(TableName.valueOf("user"));

        // 取得表中所有数据
        ResultScanner scanner = table.getScanner(new Bytes().toBytes(1), new Bytes().toBytes(2));
 
        // 循环输出表中的数据
        for (Result result : scanner) {

            byte[] row = result.getRow();
            System.out.println("row key is:" + new String(row));

            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {

                byte[] familyArray = cell.getFamilyArray();
                byte[] qualifierArray = cell.getQualifierArray();
                byte[] valueArray = cell.getValueArray();

                System.out.println("row value is:" + new String(familyArray) + new String(qualifierArray) 
                                                                             + new String(valueArray));
            }
        }

        System.out.println("---------------查询整表数据 END-----------------");

    }

	@After
    public void testAfter() {
		try {
			logger.debug("testAfter-------");
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
}
