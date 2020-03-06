package com.hbase.util;

import java.util.Random;

public class PhoneUtils {

    private static Random r = new Random();

    /**
     * 随机生成测试手机号码 prefix: 手机号码前缀 eq:186
     */
    public static String getPhoneNum(String prefix) {
        return prefix + String.format("%08d", r.nextInt(99999999));
    }

    /**
     * 随机生成时间
     *
     * @param year 年
     * @return 时间 格式：yyyyMMddHHmmss
     */
    public static String getDate(String year) {
        return year
                + String.format(
                "%02d%02d%02d%02d%02d",
                new Object[] { r.nextInt(12) + 1, r.nextInt(28) + 1,
                        r.nextInt(24), r.nextInt(60), r.nextInt(60) });
    }

    /**
     * 随机生成时间
     *
     * @param prefix 年月日
     * @return 时间 格式：yyyyMMddHHmmss
     */
    public static String getDate2(String prefix) {
        return prefix
                + String.format(
                "%02d%02d%02d",
                new Object[] { r.nextInt(24), r.nextInt(60), r.nextInt(60) });
    }


    /**
     * 获取通话时长，单位：秒
     * @return
     */
    public static String getCallLength(){
        return r.nextInt(99) + "";
    }

    /**
     * 获取通话类型：0主叫，1被叫
     * @return
     */
    public static String getCallType(){
        return r.nextInt(2) + "";
    }


}
