package com.hbase.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.hbase.conn.HbaseConnHelper;
import com.hbase.util.PhoneUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import com.hbase.util.HBaseToolUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseTest {

	private final static String TABLE_NAME = "test";
	private final static String COL_FAMILY = "cf";
	private final static int REGION_COUNT = 20;

	public static void main(String[] args) throws Exception{
		
		//HBaseToolUtil.truncateTable("test");
		
		Date startTime;
		startTime = new Date();
		
		//insertPhoneNumToDB();
		
		showSpendTime(startTime);
		startTime = new Date();

		// 创建表
//		if (HBaseToolUtil.isExistTable(TABLE_NAME)) {
//			HBaseToolUtil.deleteTable(TABLE_NAME);
//		}
//		HBaseToolUtil.createTable(TABLE_NAME, REGION_COUNT, COL_FAMILY);


		// 插入测试数据，这里不建议使用insertToDB1接口
		//insertPhoneNumToDB();

		//HBaseToolUtil.getTableRowCount(TABLE_NAME);
		showSpendTime(startTime);
		startTime = new Date();

		/*
			 1_18643853221_9223370529181710807 column=cf:date, timestamp=1583394294326, value=20171011060425
			 1_18643853221_9223370529181710807 column=cf:dnum, timestamp=1583394294326, value=17757540719
			 1_18643853221_9223370529181710807 column=cf:length, timestamp=1583394294326, value=27
			 1_18643853221_9223370529181710807 column=cf:type, timestamp=1583394294326, value=0

			 1_18643853221_9223370537363617807 column=cf:date, timestamp=1583394294326, value=20170708131918
			 1_18643853221_9223370537363617807 column=cf:dnum, timestamp=1583394294326, value=17777068301
			 1_18643853221_9223370537363617807 column=cf:length, timestamp=1583394294326, value=9
			 1_18643853221_9223370537363617807 column=cf:type, timestamp=1583394294326, value=0

			 1_18643853221_9223370540956262807 column=cf:date, timestamp=1583394294326, value=20170527232153
			 1_18643853221_9223370540956262807 column=cf:dnum, timestamp=1583394294326, value=17782139298
			 1_18643853221_9223370540956262807 column=cf:length, timestamp=1583394294326, value=8
			 1_18643853221_9223370540956262807 column=cf:type, timestamp=1583394294326, value=0

			 1_18643853221_9223370553091696807 column=cf:date, timestamp=1583394294326, value=20170107122439
			 1_18643853221_9223370553091696807 column=cf:dnum, timestamp=1583394294326, value=17776073860
			 1_18643853221_9223370553091696807 column=cf:length, timestamp=1583394294326, value=56
			 1_18643853221_9223370553091696807 column=cf:type, timestamp=1583394294326, value=1
		 */

		String rowKey = "1_18643853221_9223370529181710807";
		Result result = HBaseToolUtil.getOneRow(TABLE_NAME, rowKey);
		if (!result.isEmpty()){
			byte[] family = COL_FAMILY.getBytes();
			System.out.print(new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "dnum".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "date".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "type".getBytes()))));
			System.out.println(" - " + new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "length".getBytes()))));
		} else {
			System.out.println("rowkey:" + rowKey + " 无数据");
		}



		showSpendTime(startTime);
		startTime = new Date();

		scanPhoneRecord("18643853221", "10", "11");

		showSpendTime(startTime);
		startTime = new Date();

		System.out.println("============================00");

		scanDBPhoneNumByFilter("18699967612");

		showSpendTime(startTime);
		startTime = new Date();


		//HBaseToolUtil.flush(TABLE_NAME);

		HbaseConnHelper.closeConnection();

		testRowKeyLength();
	}
	
	private static void showSpendTime(Date startTime){
		Date endTime = new Date();
	    long spendTime = endTime.getTime() - startTime.getTime();
		System.out.println("spendTime:" + spendTime);
	}
	
	@SuppressWarnings("unused")
	private static void insertToDB1() throws Exception{

		// 创建表
		HBaseToolUtil.createTable(TABLE_NAME, REGION_COUNT, COL_FAMILY);
		
		// 插入数据
		List<Put> list = new ArrayList<Put>();
		String rowKeyStr = "r1";
		Put put = new Put(rowKeyStr.getBytes());
		put.addColumn(COL_FAMILY.getBytes(), "name".getBytes(), "zhaoliu1".getBytes()) ;
		list.add(put);
		
		put.addColumn(COL_FAMILY.getBytes(), "addr".getBytes(), "shanghai1".getBytes()) ;
		list.add(put);
		
		put.addColumn(COL_FAMILY.getBytes(), "age".getBytes(), "30".getBytes()) ;
		list.add(put);
		
		put.addColumn(COL_FAMILY.getBytes(), "tel".getBytes(), "13567882341".getBytes()) ;
		list.add(put);
		
		HBaseToolUtil.savePutList(list, TABLE_NAME);
		HBaseToolUtil.savePut(put, TABLE_NAME);
		
		HBaseToolUtil.insert(TABLE_NAME, "testrow", COL_FAMILY, "age", "35") ;
		HBaseToolUtil.insert(TABLE_NAME, "testrow", COL_FAMILY, "cardid", "12312312335") ;
		HBaseToolUtil.insert(TABLE_NAME, "testrow", COL_FAMILY, "tel", "13512312345") ;
		
		HBaseToolUtil.insert(TABLE_NAME, "2014-01-01", COL_FAMILY, "age", "99") ;
		HBaseToolUtil.insert(TABLE_NAME, "2014-01-01", COL_FAMILY, "cardid", "11312312335") ;
		HBaseToolUtil.insert(TABLE_NAME, "2014-02-01", COL_FAMILY, "tel", "11512312345") ;
		
		List<Result> resultList = HBaseToolUtil.getRows(TABLE_NAME, "2014-01", COL_FAMILY, new String[]{"age"}) ;
		for(Result rs : resultList)
		{
			String rowKey = new String(rs.getRow(), "UTF-8"); 
			System.out.println("row key is:" + new String(rowKey));
			List<Cell> cells = rs.listCells(); 
			for (Cell cell : cells) { 
				
				String family = new String(CellUtil.cloneFamily(cell), "UTF-8"); 
				String qualifier = new String(CellUtil.cloneQualifier(cell),"UTF-8"); 
				String value = new String(CellUtil.cloneValue(cell), "UTF-8"); 
				System.out.println(":::::[row:" + rowKey + "],[family:" + family 
				         + "],[qualifier:" + qualifier + "],[value:" + value + "]"); 
			}
		}
		
		Result result = HBaseToolUtil.getOneRow(TABLE_NAME, "2014-01-01");

		String rowKey = new String(result.getRow(), "UTF-8"); 
		System.out.println("row key is:" + new String(rowKey));
		List<Cell> cells = result.listCells(); 
		for (Cell cell : cells) { 
			
			String family = new String(CellUtil.cloneFamily(cell), "UTF-8"); 
			String qualifier = new String(CellUtil.cloneQualifier(cell),"UTF-8"); 
			String value = new String(CellUtil.cloneValue(cell), "UTF-8"); 
			System.out.println(":::::[row:" + rowKey + "],[family:" + family 
			         + "],[qualifier:" + qualifier + "],[value:" + value 
			         + "]"); 

		}
	}
	
	/**
	 * 通话详单： 手机号 对方手机号 通话时间 通话时长 主叫被叫
	 * 查询通话详：查询某一个月  某个时间段  所有的通话记录（时间降序）
	 * Rowkey：手机号_（Long.max-通话时间)
	 */
	
	private static PhoneUtils phoneUtils = new PhoneUtils();
	private static Random r = new Random();

	private static void testRowKeyLength() throws ParseException {
		String date2020 = "20200101000000";
		String date2030 = "20300101000000";
		String date2050 = "20500101000000";
		String date2120 = "21200101000000";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

		String tt2020 = "" + (Long.MAX_VALUE - sdf.parse(date2020).getTime());
		String tt2030 = "" + (Long.MAX_VALUE - sdf.parse(date2030).getTime());
		String tt2050 = "" + (Long.MAX_VALUE - sdf.parse(date2050).getTime());
		String tt2120 = "" + (Long.MAX_VALUE - sdf.parse(date2120).getTime());

		/**
		 * tt2020:9223370459046775807
		 * tt2050:9223369512275575807
		 * tt2120:9223367303373175807
		 */
		System.out.println("tt2020:" + tt2020);
		System.out.println("tt2030:" + tt2030);
		System.out.println("tt2050:" + tt2050);
		System.out.println("tt2120:" + tt2120);


		System.out.println("01_002003_70459046775807".length());
		byte[] bytes = Bytes.toBytes("01_002008_70459046775807");
		System.out.println(bytes.length);

	}
	@SuppressWarnings("unused")
	private static void insertPhoneNumToDB() throws Exception{
		byte[] family = COL_FAMILY.getBytes();
		
		List<Put> puts = new ArrayList<Put>();
		
		for (int i = 0; i < 3; i++) {
			String pnum = phoneUtils.getPhoneNum("186");
			
			for (int j = 0; j < 4; j++) {
				// 通话号码：17796695196
				String dnum = phoneUtils.getPhoneNum("177");
				// 通话时长：20171213212605   2017年12月13日21时26分05秒
				String datestr = phoneUtils.getDate("2017");
				// 通话时长：56秒
				String length = r.nextInt(99) + "";
				// 通话类型：0主叫，1被叫
				String type = r.nextInt(2) + "";
			
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				// Rowkey：手机号_（Long.max-通话时间)
				// 18685184797_9223370523683210807
				// Long.MAX_VALUE(922337^2036854775807) - 1513171565000(20171213212605)
				// 18685184797_9223370523688018807

				int regionNum = HBaseToolUtil.genRegionNum(pnum, REGION_COUNT);
				String rowkey = regionNum + "_" + pnum + "_" + (Long.MAX_VALUE-sdf.parse(datestr).getTime());
				Put put = new Put(rowkey.getBytes());
				put.addColumn(family, "dnum".getBytes(), dnum.getBytes());
				put.addColumn(family, "date".getBytes(), datestr.getBytes());
				put.addColumn(family, "length".getBytes(), length.getBytes());
				put.addColumn(family, "type".getBytes(), type.getBytes());
				
				puts.add(put);
			}
			
			if (puts.size() > 1000){
				HBaseToolUtil.savePutList(puts, "test");
				
				puts.clear();
			}
			
		}
		HBaseToolUtil.savePutList(puts, "test");
	}

	
	public static void scanPhoneRecord(String phoneNum, String startMonth, String endMonth) throws Exception {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		// 范围查找
		// 起始位置
		// 随机生成的电话号码，可能需要重新从scan 'phone'进行获取
		int regionNum = HBaseToolUtil.genRegionNum(phoneNum, REGION_COUNT);
		String startRowkey = regionNum + "_" + phoneNum + "_" + (Long.MAX_VALUE - sdf.parse("2017" + endMonth + "01000000").getTime());
		// 结束位置
		String stopRowkey = regionNum + "_" + phoneNum + "_" + (Long.MAX_VALUE - sdf.parse("2017" + startMonth + "01000000").getTime());
		
		List<Result> results = HBaseToolUtil.getRows("test", startRowkey, stopRowkey);
		
		byte[] family = COL_FAMILY.getBytes();
		for (Result rs : results) {
			System.out.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "dnum".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "date".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "type".getBytes()))));
			System.out.println(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "length".getBytes()))));
		}
	}
	
	
	public static void scanDBPhoneNumByFilter(String phoneNum) throws Exception {
		
		
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		// 前缀过滤器
		PrefixFilter filter1 = new PrefixFilter(phoneNum.getBytes());
		list.addFilter(filter1);
		
		
		byte[] family = COL_FAMILY.getBytes();
		
		//SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
		//		family, "type".getBytes(), CompareOp.EQUAL, "0".getBytes());
		//list.addFilter(filter2);
		
		ResultScanner rss = HBaseToolUtil.scanRowByFilterList("test", list);
		for (Result rs : rss) {
			System.out.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "dnum".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "date".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "type".getBytes()))));
			System.out.println(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "length".getBytes()))));
		}
	}
}
