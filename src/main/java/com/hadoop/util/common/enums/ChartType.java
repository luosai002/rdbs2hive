package com.hadoop.util.common.enums;

/**
 * Created by sai.luo on 2017-7-14.
 */

public enum ChartType {
    /**
     * 九宫格
     */
    Grid(1),//九宫格
    Scatter(2),//条形图
    BarS(3),//堆叠柱状图
    Bar(4),//柱状图
    Pie(5);//饼图
    private int value ;

    ChartType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
