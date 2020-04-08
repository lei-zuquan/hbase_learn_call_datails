package com.hbase.test;

import com.hbase.util.HBaseUtil;

import java.util.concurrent.TimeUnit;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-04-03 10:13
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class MyThreadWriteDataToHbase extends Thread {
    //private String name;

    public MyThreadWriteDataToHbase(String name) {
        super(name);
    }

    @Override
    public void run() {
        try {
            HBaseUtil.makeHbaseConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int j = 0; j < 1; j++) {
            HBaseUtilTest.insertOneDataToHbase(getName());

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }


}
