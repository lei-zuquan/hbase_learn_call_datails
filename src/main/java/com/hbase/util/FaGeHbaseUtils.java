package com.hbase.util;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
//import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FaGeHbaseUtils implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private Connection conn = null;
	
	public FaGeHbaseUtils() {};

	/**
	 * 连接集群
	 * @return
	 */
	public void connect() {
		try {
	        Configuration config = HBaseConfiguration.create();
	        String zkAddress = "node-03.stcn.com,node-01.stcn.com,node-02.stcn.com";
	        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
			this.conn = ConnectionFactory.createConnection(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 关闭连接
	 */
	public void close() {
		try {
			this.conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 插入一个rowkey的数据
	 * @param tableName
	 * @param rowKey
	 * @param data Map<"列族名称",Map<"字段名称","字段值">>
	 * @return
	 */
	public int putByRowKey(String tableName, String rowKey, Map<String,Map<String,String>> data) {
		int num = 1;
		try {
			TableName tablename = TableName.valueOf(tableName);
	        Put put = new Put(rowKey.getBytes());
	        for (Map.Entry<String,Map<String,String>> dataEntry : data.entrySet()) {
	          String columnsFamilyName = dataEntry.getKey();
	          Map<String,String> columnsMap = dataEntry.getValue();
	          for (Map.Entry<String,String> columnEntry : columnsMap.entrySet()) {
	        	  String columnName = columnEntry.getKey();
	        	  String columnValue = columnEntry.getValue();
	        	  put.addColumn(columnsFamilyName.getBytes(), columnName.getBytes(), columnValue.getBytes()) ;
	          }
	        }
			Table table = this.conn.getTable(tablename);
	        table.put(put);
		} catch (IOException e) {
			num = 0;
			e.printStackTrace();
		}
		return num;		
	}
	/**
	 * 判断rowkey是否存在
	 * @param tableName
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
    public boolean isExistRowKey(String tableName, String rowKey){
        boolean status = false;
		try {
			Table table = this.conn.getTable(TableName.valueOf(tableName));
	        Get get = new Get(rowKey.getBytes());
	        status = table.exists(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
        return status;
    }
    /**
	* 判断表的列族是否存在
     * @param tableName
     * @param cf
     * @return
     * @throws IOException
     */
    public boolean isExistColumnFamily(String tableName, String cf){
    	boolean status = false;
    	try {
            if(isExistTable(tableName)) {
                Table table = this.conn.getTable(TableName.valueOf(tableName));
                HTableDescriptor hTableDescriptor  = table.getTableDescriptor();
                HColumnDescriptor hColumnDescriptor  = hTableDescriptor.getFamily(Bytes.toBytes(cf));
                status = hColumnDescriptor ==null?false:true;

            }
    	}catch (IOException e) {
			e.printStackTrace();
		}
    	return status;
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean isExistTable(String tableName) {
    	boolean status = false;
		try {
			Admin admin = this.conn.getAdmin();
			status = admin.tableExists(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
        return status;
    }
    /**
     * 根据rowkey前缀字符串查找
     * @param tableName
     * @param prefix
     * @return
     * @throws IOException
     */
    public Map<String,List<Cell>> scanRowBykeyPrefix(String tableName, String prefix) {
        Map<String,List<Cell>> map = new HashMap<>();
    	try {
        	Scan scan = new Scan();
        	Table table = this.conn.getTable(TableName.valueOf(tableName));
        	ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(prefix));
        	scan.setFilter(filter);
        	ResultScanner scanner = table.getScanner(scan);
            for(Result result:scanner){
                map.put(Bytes.toString(result.getRow()),result.listCells());
            }
            table.close();
    	} catch (IOException e) {
			e.printStackTrace();
		}
        return map;
    }
    /**
     * 根据rowkey前缀查询指定列的值
     * @param tableName
     * @param prefix
     * @param family
     * @param column
     * @return
     */
    public List<String> scanColumnBykeyPrefix(String tableName, String prefix,String family,String column) {
        List<String> resp = new ArrayList<String>();
    	try {
        	Scan scan = new Scan();
        	Table table = this.conn.getTable(TableName.valueOf(tableName));
        	scan.setFilter(new PrefixFilter(prefix.getBytes()));
        	ResultScanner scanner = table.getScanner(scan);
            for(Result result:scanner){
            	resp.add(Bytes.toString(result.getValue(family.getBytes(), column.getBytes())));
            }
            table.close();
    	} catch (IOException e) {
			e.printStackTrace();
		}
        return resp;
    }
    /**
     *  根据rowkey前缀查询指定列的值,并且排除特定值
     * @param tableName
     * @param prefix
     * @param family
     * @param column
     * @param excludeValues
     * @return
     */
    public List<String> scanColumnBykeyPrefix(String tableName, String prefix,String family,String column,List<String> excludeValues) {
        List<String> resp = new ArrayList<String>();
    	try {
        	Scan scan = new Scan();
        	Table table = this.conn.getTable(TableName.valueOf(tableName));
        	scan.setFilter(new PrefixFilter(prefix.getBytes()));
        	ResultScanner scanner = table.getScanner(scan);
            for(Result result:scanner){
            	String row = Bytes.toString(result.getValue(family.getBytes(), column.getBytes()));
            	if(!excludeValues.contains(row)) {
            		resp.add(row);
            	}
            }
            table.close();
    	} catch (IOException e) {
			e.printStackTrace();
		}
        return resp;
    }
    /**
     * 根据rowkey，获取所有列族和列数据
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public List<Cell> scanRowByKey(String tableName,String rowKey){
    	List<Cell> list = null;
    	try {
            Table table = this.conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            list = result.listCells();
            table.close();
    	} catch (IOException e) {
			e.printStackTrace();
		}
        return list;
    }
    /**
     * 获取某个rowkey下某列族的某列值
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @return
     * @throws IOException
     */
    public String scanColumnByKey(String tableName,String rowKey,String family,String column){
    	String resp = "";
    	try {
            Table table = this.conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            resp = Bytes.toString(result.getValue(family.getBytes(), column.getBytes()));
            table.close();
    	} catch (IOException e) {
			e.printStackTrace();
		}
        return resp;
    }
    /**
     * 获取某个rowkey下某列族的某列,如果存在则返回值
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @return
     */
    public Map<String,Object> getColumnByKey(String tableName,String rowKey,String family,String column){
    	Map<String,Object> resp = new HashMap<String,Object>();
    	try {
            Table table = this.conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            if(result.containsColumn(family.getBytes(), column.getBytes())) {
            	resp.put("status", true);
                resp.put("value", Bytes.toString(result.getValue(family.getBytes(), column.getBytes())));
            }else {
            	resp.put("status", false);
            }
            table.close();
    	} catch (IOException e) {
			e.printStackTrace();
		}
        return resp;
    }
	
}
