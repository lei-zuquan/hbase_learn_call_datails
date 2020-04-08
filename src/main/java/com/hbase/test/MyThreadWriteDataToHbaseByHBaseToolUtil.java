package com.hbase.test;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-04-03 10:13
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class MyThreadWriteDataToHbaseByHBaseToolUtil extends Thread {
    //private String name;

    public MyThreadWriteDataToHbaseByHBaseToolUtil(String name) {
        super(name);
    }

    @Override
    public void run() {
        try {
            //HbaseUtil.makeHbaseConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int j = 0; j < 20; j++) {
            HBaseToolUtilTest.insertOneDataToHbaseByHBaseToolUtil(getName());
            try {
                //TimeUnit.SECONDS.sleep(2);
            } catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }


}
