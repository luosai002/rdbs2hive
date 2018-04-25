package com.hadoop.util.common;

/**
 * Created by sai.luo on 2017-5-16.
 */
public class DBType {
    public static final int Int_TYPE = 1 ;
    public static final int BigInt_TYPE = 2 ;
    public static final int Double_TYPE = 3 ;
    public static final int Date_TYPE = 4 ;
    public static final int TimeStamp_TYPE = 5 ;
    public static final int String_TYPE = 6 ;


    public static String getDBType(int dbType){
        switch (dbType){
            case Int_TYPE :
                    return  "int";
            case BigInt_TYPE:
                return "bigint";
            case Double_TYPE :
                return "double";
            case Date_TYPE :
                return "date";
            case TimeStamp_TYPE:
                return "timestamp";
            case String_TYPE:
                return "string" ;
            default:
                return "string" ;
        }
    }

    public static int getDBType(String str){
        if (NumberValidationUtils.isWholeNumber(str)){
            try {
                Integer.valueOf(str);
                return Int_TYPE ;
            }catch (Exception e){
                return BigInt_TYPE ;
            }
        }
        if (NumberValidationUtils.isDecimal(str)){
            return Double_TYPE;
        }
        if (DateTimeUtil.isDateString(str)){
            return Date_TYPE;
        }
        if (DateTimeUtil.isDateTimeString(str)){
            return TimeStamp_TYPE ;
        }
        return String_TYPE;
    }


}
