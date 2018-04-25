package com.hadoop.util.common.excelUtil;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by kanghong.zhao on 2016-8-22.
 */
public class ExcelWriter {

    /**
     * 写入07excel
     */
    public static XSSFWorkbook write2007Excel(List<Map<String, Object>> list, String []keys, String columnNames[]) {
            //创建excel文件对象
            XSSFWorkbook wb = new XSSFWorkbook();
            //创建一个张表
            XSSFSheet sheet = wb.createSheet();
            //创建行对象
            XSSFRow row = null;
            //创建表格对象
            XSSFCell cell = null;
            //循环行
            write(wb, sheet, list, keys, columnNames);
            return wb;
    }

    /**

     * 寫入03excel
     */

    public static HSSFWorkbook write2003Excel(List<Map<String, Object>> list, String []keys, String columnNames[]) {
            //创建excel文件对象
            HSSFWorkbook wb = new HSSFWorkbook();
            //创建一个张表
            HSSFSheet sheet = wb.createSheet();
            //创建行对象
            HSSFRow row = null;
            //创建表格对象
            HSSFCell cell = null;
            //循环行
            write(wb, sheet, list, keys, columnNames);
            return wb;

    }

    private static void write(Workbook wb, Sheet sheet, List<Map<String, Object>> list, String []keys, String columnNames[]){
        for(int i=0;i<keys.length;i++){
            sheet.setColumnWidth((short) i, (short) (35.7 * 150));
        }

        // 创建第一行
        Row row = sheet.createRow((short) 0);

        // 创建两种单元格格式
        CellStyle cs = wb.createCellStyle();
        CellStyle cs2 = wb.createCellStyle();

        // 创建两种字体
        Font f = wb.createFont();
        Font f2 = wb.createFont();

        // 创建第一种字体样式（用于列名）
        f.setFontHeightInPoints((short) 10);
        f.setColor(IndexedColors.BLACK.getIndex());
        f.setBoldweight(Font.BOLDWEIGHT_BOLD);

        // 创建第二种字体样式（用于值）
        f2.setFontHeightInPoints((short) 10);
        f2.setColor(IndexedColors.BLACK.getIndex());

//        Font f3=wb.createFont();
//        f3.setFontHeightInPoints((short) 10);
//        f3.setColor(IndexedColors.RED.getIndex());

        // 设置第一种单元格的样式（用于列名）
        cs.setFont(f);
        cs.setBorderLeft(CellStyle.BORDER_THIN);
        cs.setBorderRight(CellStyle.BORDER_THIN);
        cs.setBorderTop(CellStyle.BORDER_THIN);
        cs.setBorderBottom(CellStyle.BORDER_THIN);
        cs.setAlignment(CellStyle.ALIGN_CENTER);

        // 设置第二种单元格的样式（用于值）
        cs2.setFont(f2);
        cs2.setBorderLeft(CellStyle.BORDER_THIN);
        cs2.setBorderRight(CellStyle.BORDER_THIN);
        cs2.setBorderTop(CellStyle.BORDER_THIN);
        cs2.setBorderBottom(CellStyle.BORDER_THIN);
        cs2.setAlignment(CellStyle.ALIGN_CENTER);
        //设置列名
        for(int i=0;i<columnNames.length;i++){
            Cell cell = row.createCell(i);
            cell.setCellValue(columnNames[i]);
            cell.setCellStyle(cs);
        }
        //设置每行每列的值
        for (short i = 1; i < list.size(); i++) {
            // Row 行,Cell 方格 , Row 和 Cell 都是从0开始计数的
            // 创建一行，在页sheet上
            Row row1 = sheet.createRow(i);
            // 在row行上创建一个方格
            for(short j=0;j<keys.length;j++){
                Cell cell = row1.createCell(j);
                cell.setCellValue(list.get(i).get(keys[j]) == null?" ": list.get(i).get(keys[j]).toString());
                cell.setCellStyle(cs2);
            }
        }
    }

    /**

     * 对外提供读取excel 的方法

     */

    public static Workbook writeExcel(List<String> mList) throws IOException {
//        String fileName = file.getName();
//        String extension = fileName.lastIndexOf(".") == -1 ? "" : fileName.substring(fileName.lastIndexOf(".") + 1);
//        //判断文件类型
//        if ("xls".equals(extension)) {
//            return write2003Excel(mList);
//        } else if ("xlsx".equals(extension)) {
//            return write2007Excel(mList);
//        } else {
//            throw new IOException("不支持的文件类型");
//        }
        return null;
    }

    public static void main(String[] args) throws IOException {
       /* String columnNames[]={"ID","项目名","销售人"};//列名
        String keys[]    =     {"id","name","saler"};//map中的key

        List<Map<String, Object>> list = new ArrayList<>();

        Map<String, Object> sheet = new HashMap<>();
        sheet.put("sheetName", "第一页");
        list.add(sheet);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("id", 1);
        map1.put("name", "好莱坞");
        map1.put("saler", "陈好");
        list.add(map1);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("id", 2);
        map2.put("name", "好莱坞2");
        map2.put("saler", "陈好2");
        list.add(map2);

        Workbook wb = write2007Excel(list, keys, columnNames);
        FileOutputStream out = new FileOutputStream("d://tmp.xlsx");
        wb.write(out);
        out.close();
*/

       String [][] str = {{"a","b","c"},{"aa","bb","cc"},{"aaa","bbb","ccc"}};
        List<List<String>> collect = Arrays.stream(str).map(strings -> Arrays.asList(strings)).collect(Collectors.toList());
        System.out.println(collect);
    }

    public static Workbook mockWorkbook(){
        String columnNames[]={"ID","项目名","销售人"};//列名
        String keys[]    =     {"id","name","saler"};//map中的key

        List<Map<String, Object>> list = new ArrayList<>();

        Map<String, Object> sheet = new HashMap<>();
        sheet.put("sheetName", "第一页");
        list.add(sheet);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("id", 1);
        map1.put("name", "好莱坞");
        map1.put("saler", "陈好");
        list.add(map1);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("id", 2);
        map2.put("name", "好莱坞2");
        map2.put("saler", "陈好2");
        list.add(map2);

        Workbook wb = write2007Excel(list, keys, columnNames);
        return wb;
    }


}
