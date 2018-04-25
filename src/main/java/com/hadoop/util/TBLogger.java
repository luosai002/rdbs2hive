package com.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sai.luo on 2017-6-23.
 */
public class TBLogger {
    private Logger logger ;
    public void errer(Exception e){
        logger.error(e.getCause().getLocalizedMessage());
        StackTraceElement[] stackTrace = e.getStackTrace();
        for (int i=0;i<stackTrace.length;i++){
            logger.error(stackTrace[i].toString());
        }
    }
    private TBLogger(){

    }
    public static TBLogger getInstance(Class<?> clazz){
        TBLogger tbLogger = new TBLogger();
        tbLogger.logger = LoggerFactory.getLogger(clazz);
        return tbLogger;
    }
    public void error(String error){
        logger.error(error);
    }
    public void info(String info){
        logger.info(info);
    }
}
