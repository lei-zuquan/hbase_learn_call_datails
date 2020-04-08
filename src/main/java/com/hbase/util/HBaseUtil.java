package com.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;


/**
 * Hbase操作工具类
 *
 * 工具类，异常一般不用自己做处理
 *
 */
public class HBaseUtil {

    // ThreadLocal，线程存储中有一块内存空间可以放数据ThreadLocal。解决线程操作hbase
    // ThreadLocal解决了同一个线程中共享数据问题，效率会更高
    // ThreadLocal解决不了多线程安全
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<>();

    //private static Connection conn = null;

    private HBaseUtil() {

    }

    /**
     * 获取Hbase连接对象
     * @return
     * @throws Exception
     */
    public static void makeHbaseConnection() throws Exception {
        //Configuration conf = HBaseConfiguration.create();
        //conn = ConnectionFactory.createConnection(conf);
        //return conn;
        Connection conn = connHolder.get();
        if (conn == null){
            Configuration conf = HBaseConfiguration.create();
            //String zk_list = "node-03,node-01,node-02";
            //conf.set("hbase.zookeeper.quorum", zk_list);
            // 设置连接参数:hbase数据库使用的接口
            //conf.set("hbase.zookeeper.property.clientPort", "2181");
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
    }

    /**
     * 获取数据库元数操作对象
     * @return
     */
    public static Admin getHAdmin(){
        Admin hadmin = null;
        try {
            hadmin = connHolder.get().getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hadmin;
    }
    /**
     * 生成分区键
     * @param regionCount
     * @return
     */
    public static byte[][] genRegionKeys(int regionCount){
        byte[][] bs = new byte[regionCount - 1][];

        // 3个分区 ==》对应2个分区键 ==》0，1
        for (int i = 0; i < regionCount - 1; i++) {
            bs[i] = Bytes.toBytes(i + "|");
        }

        return bs;
    }

    /**
     * 生成分区号
     * @param rowkey
     * @param regionCount
     * @return
     */
    public static int genRegionNum(String rowkey, int regionCount){
        int regionNum;
        int hash = rowkey.hashCode();

        if (regionCount > 0 && (regionCount & (regionCount - 1)) == 0){
            // 2 n
            regionNum = hash & (regionCount - 1);
        } else {
            regionNum = hash % (regionCount);
        }
        return regionNum;// + "_" + rowkey;
    }

    /**
     * 反转rowkey字符串
     * @param rowKey
     * @return
     */
    public static String reverseRowkey(String rowKey){
        return new StringBuilder(rowKey).reverse().toString();
    }

    public static void main(String[] args) {
        /**
         * 测试分区号
         */
        System.out.println(genRegionNum("lisi1", 3));
        System.out.println(genRegionNum("lisi2", 3));
        System.out.println(genRegionNum("lisi3", 3));
        System.out.println(genRegionNum("lisi4", 3));
        System.out.println(genRegionNum("lisi5", 3));
        System.out.println(genRegionNum("lisi6", 3));
        System.out.println(genRegionNum("lisi7", 3));

        /**
         * 测试分区键

        byte[][] bytes = genRegionKeys(6);
        for (byte[] aByte : bytes) {
            System.out.println(Bytes.toString(aByte));
        }
         */

        /**
         * 测试反转rowkey字符串
         */
        System.out.println(reverseRowkey("zhangsan121"));
    }



    /**
     * 创建表，可以同时创建多个列簇
     *
     * @param tableName
     * @param columnFamily
     */
    public static void createTable(String tableName, int regionCount, int maxVersion, int timeToLive, String... columnFamily) {
        TableName tableNameObj = TableName.valueOf(tableName);
        Admin admin = getHAdmin();
        try {
            if (admin.tableExists(tableNameObj)) {
                System.out.println("Table : " + tableName + " already exists !");
            } else {
                HTableDescriptor td = new HTableDescriptor(tableNameObj);
                int len = columnFamily.length;
                for (int i = 0; i < len; i++) {
                    HColumnDescriptor family = new HColumnDescriptor(columnFamily[i]);
                    // 指定保存版本号，默认版本号为：1
                    family.setMaxVersions(maxVersion);
                    // TTL参数的单位是秒，默认值是Integer.MAX_VALUE，即2^31-1=2 147 483 647 秒，大约68年。使用TTL默认值的数据可以理解为永久保存。
                    family.setTimeToLive(timeToLive); // 单元秒
                    td.addFamily(family);
                }
                // 通过分区数得到分区键
                byte[][] bs = genRegionKeys(regionCount);
                admin.createTable(td, bs);

                System.out.println(tableName + " 表创建成功！");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(tableName + " 表创建失败！");
        }
    }



    /**
     * 通过表名，获取表对象
     * @param tableName
     * @return
     */
    public static Table getTable(String tableName){
        Table table = null;

        try {
            table = connHolder.get().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }


    public static void closeTable(Table table){
        if (table != null){
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void insertData(String tableName, String rowKey, String family, String column, String value) throws Exception{
        Table table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));

        table.put(put);
        table.close();
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isExistTable(String tableName) throws IOException {
        return getHAdmin().tableExists(TableName.valueOf(tableName));
    }
    /**
     * 查询表中的数据
     */
    public static int getTableRowCount(String tableName)
            throws IOException {
        System.out.println("--------------------查询整表的行的数量--------");

        // 获取数据表对象
        Table table = getTable(tableName);

        // 获取表中的数据
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner scanner = table.getScanner(scan);

        int rowCount = 0;
        for (Result result : scanner) {
            rowCount += result.size();
        }
        System.out.println("---------------查询整表数据结束----------Count:" + rowCount);
        return rowCount;
    }


    /**
     * 创建命名空间
     * @param namespace
     * @throws IOException
     */
    public static void createNamespace(String namespace) throws IOException {
        Admin admin = getHAdmin();

        try{
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(namespace);
            if (namespaceDescriptor != null) {
            }
        }catch(NamespaceNotFoundException e){
            //若发生特定的异常，即找不到命名空间，则创建命名空间
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }


    /**
     * 删除指定的表，输入值为表名。Hbase是区分大小写的
     *
     * @param tableNameString
     * @throws IOException
     */
    public static void deleteTable(String tableNameString) throws IOException {
        System.out.println("-----------------------删除表---------------");

        // 将string转为TableName对象
        TableName tableName = TableName.valueOf(tableNameString);
        Admin admin = getHAdmin();
        // 判断表是否存在
        if (admin.tableExists(tableName)) {
            System.out.println(tableNameString + ":表存在！");

            // 设置表的状态为无效
            admin.disableTable(TableName.valueOf(tableNameString));

            // 删除指定的表
            admin.deleteTable(TableName.valueOf(tableNameString));
        } else {
            System.out.println(tableNameString + ":表不存在");
        }

        System.out.println("-------------------------删除表-----------------------");

    }

    /**
     * 这是清空表的函数，用以使表变得无效
     *
     * @param tableNameString
     * @throws IOException
     */
    public static void truncateTable(String tableNameString) throws IOException {

        System.out.println("-------------------------清空表开始------------------");

        // 取得目标数据表的表明对象
        TableName tableName = TableName.valueOf(tableNameString);

        // 设置表状态为无效
        Admin admin = getHAdmin();
        admin.disableTable(tableName);
        // 清空指定表的数据
        admin.truncateTable(tableName, true);
        // 设置表状态为有效
        //admin.enableTable(tableName);

        System.out.println("-------------------------清空表结束-----------------");
    }

    public static void flush(String tableNameString) throws Exception{
        // 取得目标数据表的表明对象
        TableName tableName = TableName.valueOf(tableNameString);

        // 设置表状态为无效
        Admin admin = getHAdmin();
        admin.flush(tableName);
    }
    /** 判断指定表的列族是否存在
     *
     * @param tableName
     * @param cf
     * @return
     * @throws IOException
     */
    public static boolean isExistColumnFamily(String tableName, String cf)
            throws IOException {
        if (isExistTable(tableName)) {
            Table table = getTable(tableName);
            HTableDescriptor tableDescriptor = table.getTableDescriptor();

            HColumnDescriptor[] descriptorArr = tableDescriptor
                    .getColumnFamilies();

            for (HColumnDescriptor hColumnDescriptor : descriptorArr) {
                String tempColumnNameString = new String(
                        hColumnDescriptor.getName());
                if (tempColumnNameString.equals(cf)) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }
    /**
     * 新建一个列簇，第一个是表名，第二个是列簇名
     *
     * @param tableNameString
     * @param columnFamily
     * @throws IOException
     */
    public static void addColumnFamily(String tableNameString,
                                       String columnFamily) throws IOException {

        if (isExistColumnFamily(tableNameString, columnFamily)) {
            System.out.println("表：" + tableNameString + " 列族：" + columnFamily
                    + "已存在");
            return;
        }

        System.out.println("新建列簇开始");

        // 取得目标数据表的标明对象
        TableName tableName = TableName.valueOf(tableNameString);

        // 创建列簇对象
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);

        // 将新建的加入到指定的数据表
        getHAdmin().addColumn(tableName, columnDescriptor);

        System.out.println("新建列簇结束");
    }

    /**
     * 删除列簇的函数，第一个是表名，第二个是列簇名
     *
     * @param tableNameString
     * @param columnFamily
     * @throws IOException
     */
    public static void DeleteColumnFamily(String tableNameString,
                                          String columnFamily) throws IOException {
        System.out.println("删除列簇开始");

        // 取得目标数据表的表明对象
        TableName tableName = TableName.valueOf(tableNameString);

        // 删除指定数据表中的指定列簇
        getHAdmin().deleteColumn(tableName, columnFamily.getBytes());

        System.out.println("删除列簇成功");
    }

    /**
     *  保存数据到指定表中
     * @param put 多行数据
     * @param tableName 表名
     */
    public static void savePut(Put put, String tableName) {

        Table table = getTable(tableName);
        try {
            table.put(put) ;
            System.out.println(Thread.currentThread().getName() + " 时间：" + LocalDateTime.now().getSecond() +  " 成功插入数据到hbase:1 条");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
    }

    /**
     *  保存数据到指定表中
     * @param putList 多行数据
     * @param tableName 表名
     */
    public static void savePutList(List<Put> putList, String tableName) {
        if (putList.size() <= 0){
            return;
        }

        Table table = getTable(tableName);
        try {
            table.put(putList) ;
            System.out.println("时间：" + LocalDateTime.now() + " 线程名：" + Thread.currentThread().getName() + " 成功插入数据到hbase:" + putList.size() + " 条");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
    }

    /**
     *  将数据插入到指定表、提定rowkey、指定列族、指定列、指定值
     * @param tableName 表名
     * @param rowKey    rowKey
     * @param family    列族
     * @param quailifer 列名
     * @param value		值
     */
    public static void insert(String tableName, String rowKey, String family,
                              String quailifer, String value) {

        Table table = getTable(tableName);
        try {
            Put put = new Put(rowKey.getBytes());
            put.addColumn(family.getBytes(), quailifer.getBytes(), value.getBytes());

            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
    }

    /**
     *  将数据插入到指定表、提定rowkey、指定列族、指定列、指定值
     * @param tableName 表名
     * @param rowKey    rowKey
     * @param family	列族
     * @param quailifer 列名
     * @param value		值
     */
    public static void insert(String tableName, String rowKey, String family, String quailifer[], String value[]){

        Table table = getTable(tableName);
        try {
            Put put = new Put(rowKey.getBytes());
            // 批量添加
            for (int i = 0; i < quailifer.length; i++) {
                String col = quailifer[i];
                String val = value[i];
                put.addColumn(family.getBytes(), col.getBytes(), val.getBytes());
            }

            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
    }


    /**
     *  通过指定rowKey获取一条数据
     * @param tableName 表名
     * @param rowKey    rowKey
     * @return
     */
    public static Result getOneRow(String tableName, String rowKey) {

        Result rsResult = null;
        Table table = getTable(tableName);
        try {
            Get get = new Get(rowKey.getBytes()) ;
            rsResult = table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
        return rsResult;
    }

    /**
     *  通过指定rowKey类型，获取全部列的一个数据集
     * @param tableName 	表名
     * @param rowKeyLike	rowKey规则
     * @return
     */
    public static List<Result> getRows(String tableName, String rowKeyLike) {

        List<Result> list = null;
        Table table = getTable(tableName);
        try {
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rs : scanner) {
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
        return list;
    }

    /**
     *  通过指定rowKey类型，获取指定列的一个数据集
     * @param tableName 	表名
     * @param rowKeyLike	rowKey规则
     * @param cols			需要获取的多列名称
     * @return
     */
    public static List<Result> getRows(String tableName, String rowKeyLike, String family, String cols[]) {

        List<Result> list = null;
        Table table = getTable(tableName);
        try {
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());

            Scan scan = new Scan();
            for (int i = 0; i < cols.length; i++) {
                scan.addColumn(family.getBytes(), cols[i].getBytes());
            }
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<>();
            for (Result rs : scanner) {
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
        return list;
    }

    /**
     *  通过指定startRow、stopRow，获取指定范围的列族一个数据集
     * @param tableName		表名
     * @param family		列族名
     * @param startRow		起始rowKey
     * @param stopRow		结束rowKey
     * @return
     */
    public static List<Result> getRows(String tableName, String family, String startRow,String stopRow){

        List<Result> list = null;
        Table table = getTable(tableName);

        try {
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes(family));
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<>();
            for (Result rsResult : scanner) {
                list.add(rsResult);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public static String getColValue(Result rs, String family, String colName){
        byte[] bytes = CellUtil.cloneValue(rs.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(colName)));
        return Bytes.toString(bytes);
    }

    public static String getColValue(Result rs, byte[] family, String colName){
        byte[] bytes = CellUtil.cloneValue(rs.getColumnLatestCell(family, Bytes.toBytes(colName)));
        return Bytes.toString(bytes);
    }
    public static String getColValue(Result rs, byte[] family, byte[] colName){
        byte[] bytes = CellUtil.cloneValue(rs.getColumnLatestCell(family, colName));
        return Bytes.toString(bytes);
    }


    public static ResultScanner scanRowByFilterList(String tableName, FilterList filterList){
        ResultScanner rss = null;
        Scan scan = new Scan();
        scan.setFilter(filterList);
        try {
            rss = getTable(tableName).getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return rss;
    }
    /**
     *  通过指定类型的rowkey，删除记录
     * @param tableName		表名
     * @param rowKeyLike	rowKey规则
     */
    public static void deleteRecordsByRowKeyLike(String tableName, String rowKeyLike){

        Table table = getTable(tableName);
        try {
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            List<Delete> list = new ArrayList<>();
            for (Result rs : scanner) {
                Delete del = new Delete(rs.getRow());
                list.add(del);
            }
            table.delete(list);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
    }

    /**
     * 指定行键进行删除 ，会将指定行键的所有记录进行删除
     *
     * rowkey 是第二层，一行有很多的数据
     *
     * @throws IOException
     */
    public static void deleteRecordByRowKey(String tableName, String rowKey)
            throws IOException {
        System.out.println("删除行开始");

        // 获取待操作的数据表对象
        Table table = getTable(tableName);

        // 创建删除条件对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        table.delete(delete);

        System.out.println("删除行结束");
    }

    public static void delete(String tableName, String rowKey, String cf, String cn) throws Exception{
        // 获取待操作的数据表对象
        Table table = getTable(tableName);

        // 创建删除条件对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));  // 推荐在生产环境使用，将指定列族下的列全部版本进行删除
        //delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn)); // 生产环境慎用，它只删除一个版本数据

        table.delete(delete);
    }


    public static void printRowRecord(Result rs) {
        String rowKey = Bytes.toString(rs.getRow());

        System.out.println("row key is:" + rowKey);
        List<Cell> cells = rs.listCells();
        for (Cell cell : cells) {

            String family = Bytes.toString(CellUtil.cloneFamily(cell));       // new String(CellUtil.cloneFamily(cell), "UTF-8");
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell)); // new String(CellUtil.cloneQualifier(cell),"UTF-8");
            String value = Bytes.toString(CellUtil.cloneValue(cell));         //new String(CellUtil.cloneValue(cell), "UTF-8");
            System.out.println(":::::[row:" + rowKey + "],[family:" + family
                    + "],[qualifier:" + qualifier + "],[value:" + value + "]");
        }
    }


    public static void close() throws Exception{
        Connection conn = connHolder.get();
         if (conn != null){
             conn.close();
             connHolder.remove();
         }
    }
}
