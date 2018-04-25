package com.hadoop.util.common.exception;

/**
 * 系统异常, 业务异常借口
 * Created by eric on 16/11/10.
 */
public interface ExceptionEnums {
    public int getCode();
    public String getMessage();
}
