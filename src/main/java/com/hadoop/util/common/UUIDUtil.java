package com.hadoop.util.common;

import java.util.UUID;

/**
 * Created by sai.luo on 2017-6-2.
 */
public class UUIDUtil {
    public static String getUUIDString(){
        return UUID.randomUUID().toString().replace("-","");
    }
}
