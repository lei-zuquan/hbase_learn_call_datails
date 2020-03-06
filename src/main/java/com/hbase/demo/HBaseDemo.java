package com.hbase.demo;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.hbase.util.PhoneUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class HBaseDemo {

	// hbase测试表名
	private final static String CALL_DETAILS = "hbase_learn_call_details";
	// hbase列族名
	private final static String CALL_DETAILS_COL_FAMILY = "cf";

	// 与HBase数据库的连接对象
	private static Connection connection;
	// 数据库元数操作对象
	private static Admin hadmin;
	private static Table htable;
	private static TableName TN = TableName.valueOf(CALL_DETAILS);
	
	//HBaseAdmin hadmin;
	//HTable htable;
	//String TN = "phone";
	
	byte[] family = CALL_DETAILS_COL_FAMILY.getBytes();
	
	@Before
	public void begin() throws Exception {
		Configuration conf = new Configuration();
		// 伪分布式 ZooKeeper 一台服务器
		conf.set("hbase.zookeeper.quorum", "node-03.stcn.com,node-01.stcn.com,node-02.stcn.com");
		//conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

		connection = ConnectionFactory.createConnection(conf);
		// 取得一个数据库元数据操作对象
		hadmin = connection.getAdmin();
		htable = connection.getTable(TN);
		//hadmin = new HBaseAdmin(conf);
		//htable = new HTable(conf, TN);
	}
	
	@After
	public void end() throws Exception {
		if(hadmin != null) {
			hadmin.close();
		}
		if(htable != null) {
			htable.close();
		}
	}
	
	@Test
	public void createTable() throws Exception {
	
		if(hadmin.tableExists(TN)) {
			hadmin.disableTable(TN);
			hadmin.deleteTable(TN);
		}
		HTableDescriptor desc = new HTableDescriptor(TN);

		HColumnDescriptor cf = new HColumnDescriptor(CALL_DETAILS_COL_FAMILY);
		cf.setInMemory(true);
		
		desc.addFamily(cf);
		hadmin.createTable(desc);
	}


	
	@Test
	public void insertDB1() throws Exception {
		String rowkey = "123123";
		Put put =  new Put(rowkey.getBytes());
		put.addColumn(family, "name".getBytes(), "xiaoming".getBytes());
		put.addColumn(family, "name".getBytes(), "xiaoming".getBytes());
		put.addColumn(family, "age".getBytes(), "99".getBytes());
		//put.add(family, "name".getBytes(), "xiaoming".getBytes());
		//put.add(family, "name".getBytes(), "xiaoming".getBytes());
		//put.add(family, "age".getBytes(), "99".getBytes());
		
		htable.put(put);
	}
	
	@Test
	public void getDB1() throws Exception {
		String rowkey = "123123";
		Get get = new Get(rowkey.getBytes());
		get.addColumn(family, "name".getBytes());
		
		Result rs = htable.get(get);
		Cell cell = rs.getColumnLatestCell(family, "name".getBytes());
		
		System.out.println(new String(CellUtil.cloneValue(cell)));
	}
	
	
	/**
	 * 通话详单： 手机号 对方手机号 通话时间 通话时长 主叫被叫
	 * 查询通话详：查询某一个月  某个时间段  所有的通话记录（时间降序）
	 * Rowkey：手机号_（Long.max-通话时间)
	 */
	
	PhoneUtils phoneUtils = new PhoneUtils();
	Random r = new Random();
	
	/**
	 * 十个用户 每个用户产生一百条通话记录
	 * @throws Exception 
	 */
	@Test
	public void insertDB2() throws Exception {
		List<Put> puts = new ArrayList<Put>();
		
		for (int i = 0; i < 10; i++) {
			String pnum = phoneUtils.getPhoneNum("186");
			
			for (int j = 0; j < 100; j++) {
				String dnum = phoneUtils.getPhoneNum("177");
				String datestr = phoneUtils.getDate("2017");
				String length = r.nextInt(99) + "";
				String type = r.nextInt(2) + "";
			
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				// Rowkey：手机号_（Long.max-通话时间)
				String rowkey = pnum + "_" + (Long.MAX_VALUE-sdf.parse(datestr).getTime());
				Put put = new Put(rowkey.getBytes());
				put.addColumn(family, "dnum".getBytes(), dnum.getBytes());
				put.addColumn(family, "date".getBytes(), datestr.getBytes());
				put.addColumn(family, "length".getBytes(), length.getBytes());
				put.addColumn(family, "type".getBytes(), type.getBytes());
				
				//put.add(family, "dnum".getBytes(), dnum.getBytes());
				//put.add(family, "date".getBytes(), datestr.getBytes());
				//put.add(family, "length".getBytes(), length.getBytes());
				//put.add(family, "type".getBytes(), type.getBytes());
				
				puts.add(put);
			}
		}
		
		htable.put(puts);
	}
	
	/**
	 * 查询某个手机号 某月产生的所有的通话记录
	 * 18695956455  5月份
	 * @throws Exception 
	 */
	@Test
	public void scanDB1() throws Exception {
		Scan scan = new Scan();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		// 范围查找
		// 起始位置
		// 随机生成的电话号码，可能需要重新从scan 'phone'进行获取
		String startRowkey = "18600108874_" + (Long.MAX_VALUE-sdf.parse("20170601000000").getTime());
		// 结束位置
		String stopRowkey = "18600108874_" + (Long.MAX_VALUE-sdf.parse("20170501000000").getTime());
		
		scan.setStartRow(startRowkey.getBytes());
		scan.setStopRow(stopRowkey.getBytes());
		
		ResultScanner rss = htable.getScanner(scan);
		for (Result rs : rss) {
			System.out.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "dnum".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "date".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "type".getBytes()))));
			System.out.println(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "length".getBytes()))));
		}
	}
	
	/**
	 * 查询某个手机号  所有主叫类型type=1的通话记录
	 * 过滤器
	 * @throws Exception 
	 */
	@Test
	public void scanDB2() throws Exception {
		Scan scan = new Scan();
		
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		// 前缀过滤器
		PrefixFilter filter1 = new PrefixFilter("18695545966".getBytes());
		list.addFilter(filter1);
		
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
				family, "type".getBytes(), CompareOp.EQUAL, "0".getBytes());
		list.addFilter(filter2);
		
		scan.setFilter(list);
		ResultScanner rss = htable.getScanner(scan);
		for (Result rs : rss) {
			System.out.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "dnum".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "date".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "type".getBytes()))));
			System.out.println(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "length".getBytes()))));
		}
	}
	
	/**
	 * 删除 cell
	 */
	
}