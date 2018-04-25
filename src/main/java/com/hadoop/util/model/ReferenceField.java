package com.hadoop.util.model;

/**
 * Created by sai.luo on 2017-6-12.
 */
public class ReferenceField {
    private String sourceTableReferenceField;
    private String targetTableReferenceField ;

    public String getSourceTableReferenceField() {
        return sourceTableReferenceField;
    }

    public void setSourceTableReferenceField(String sourceTableReferenceField) {
        this.sourceTableReferenceField = sourceTableReferenceField;
    }

    public String getTargetTableReferenceField() {
        return targetTableReferenceField;
    }

    public void setTargetTableReferenceField(String targetTableReferenceField) {
        this.targetTableReferenceField = targetTableReferenceField;
    }
}
