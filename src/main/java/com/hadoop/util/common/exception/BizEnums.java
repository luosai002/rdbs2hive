package com.hadoop.util.common.exception;

/**
 * Created by eric on 16/11/10.
 */
//业务异常
public enum BizEnums implements ExceptionEnums{
    /**
     *无权限访问该URL
     **/
    CUSTOMIZE(1,""),
    UPLOADFILEERROR(300000,"文件上传失败"),
    USERNOTEXIST(300001,"用户不存在"),
    USERNAMEORPWDERROR(300002,"用户名或密码错误"),
    NOLOGIN(300003,"您尚未登录或登录已过期，请登录后再试"),
    FILEIMPORTING(300004,"导入中..."),
    FILEEXPIRE(300005,"数据已失效，请重新操作"),
    TARGETTABLEEXIST(300006,"目标表名已存在，请重新输入"),
    TARGETTABLENOTEXIST(300007,"该数据源不存在"),
    DATAVALIDATION(300008,"数据参数缺少或无效"),
    SQLVALIDATION(300009,"SQL 语句不合法"),
    FILE_FORMART_NOT_SUPPORT(300010,"仅支持xls,xlsx,csv,txt结尾的文件格式"),
    FILE_COLUMN_NOT_EQUAL(300011,"文件中列数不一致"),
    RECORDS_NOT_EXISTS(300012,"要删除的记录不存在"),
    SQL_COLUMN_NOT_EXIST(300013,"列不存在"),
    WIDGET_NAME_EXIST(300014,"名称已存在，请重新输入"),
    DIMENSION_VALUE_COUNT_TO_MANY(300015,"维度值过多，无法生成分报告"),
    GEN_SUB_REPORT_ERROE(300016,"生成报告出错"),
    SUB_REPORT_DATA_NOT_EXIST(300017,"生成分报告所需要的数据不存在"),
    SUB_REPORT_HAD_EXIST(300018,"该维度的分报告已生成过,请选择其他维度"),
    FOLDER_NAME_NOT_NULL(300019,"文件名不能为空"),
    FOLDER_HAS_EXIST(300020,"文件名重复，请重新输入"),
    DATA_ERROER(300021,"数据错误"),
    FOLDER_NOT_NULL(300022,"文件夹有数据，确定是否删除？"),
    CUSTOM_COLUMN_NAME_EXIST(300023,"名称重复，请重新输入"),
    COLUMN_CANNOT_CONVERT(300024,"该字段不能转换");

    public int code;
    public String message;

	private BizEnums(int code, String message){
        this.code = code;
        this.message = message;
    }


    @Override
    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public static BizEnums getCustomize(Integer code,String message){
        BizEnums.CUSTOMIZE.setMessage(message);
        BizEnums.CUSTOMIZE.setCode(code);
        return BizEnums.CUSTOMIZE;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public BizEnums getBizEnumsByCode(int code){
        for(BizEnums enums : BizEnums.values()){
            if(enums.getCode() == code)
                return enums;
        }
        return null;
    }
}
