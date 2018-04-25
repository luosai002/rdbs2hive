package com.hadoop.util.common.enums;

import java.util.Arrays;

/**
 * Created by sai.luo on 2017-5-23.
 * 导入数据操作类型
 */
public enum  ImportOperatorType{
    /**
     *添加记录到目标表
     */
    ADD_TYPE(1),
    /**
     *更新目标和源记录相符的记录
     */
    UPDATE_TYPE(2),
    /**
     *如果目标存在相同记录，更新它，否则，添加它
     */
    ADD_OR_UPDATE(3),
    /**
     *删除目标中和源记录相符的记录
     */
    DELETE(4),

    /**
     *删除目标全部记录，并从源重新导入
     */
    COPY(5);
    private int operateType ;



    ImportOperatorType(int operateType) {
        this.operateType = operateType;
    }

    public int getOperateType() {
        return operateType;
    }

    public static ImportOperatorType getImportOperatorType(int operateType){
        for (ImportOperatorType importOperatorType:ImportOperatorType.values()){
            if (importOperatorType.getOperateType()==operateType){
                return importOperatorType ;
            }
        }
        return null ;
    }
    public static void main(String[] args) {
        System.out.println(ImportOperatorType.ADD_TYPE);
        System.out.println(ImportOperatorType.ADD_TYPE.getOperateType());
        Arrays.asList(ImportOperatorType.values()).forEach(importOperatorType -> {
            if(importOperatorType.getOperateType()==ImportOperatorType.UPDATE_TYPE.getOperateType()){
                System.out.println("更新");
                return;
            }
        });

    }
}
