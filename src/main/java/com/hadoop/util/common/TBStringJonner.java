package com.hadoop.util.common;


/**
 * Created by sai.luo on 2017-7-28.
 */
public class TBStringJonner {
    private StringBuilder value ;
    private final String delimiter;
    public TBStringJonner(String delimiter){
        this.delimiter = delimiter ;
    }
    private StringBuilder prepareBuilder() {
        if (value != null&&value.length()>0) {
            value.append(delimiter);
        } else {
            value = new StringBuilder();
        }
        return value;
    }
    public TBStringJonner add(CharSequence newElement) {
        prepareBuilder().append(newElement);
        return this;
    }
    public TBStringJonner clear(){
        if (value!=null&&value.length()>0){
            value.delete(0,value.length());
        }
        return this;
    }

    @Override
    public String toString() {
        return value==null?null:value.toString();
    }
}
