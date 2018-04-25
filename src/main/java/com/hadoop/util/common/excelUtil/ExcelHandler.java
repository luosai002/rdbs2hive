package com.hadoop.util.common.excelUtil;

import com.hadoop.util.common.DBType;
import com.hadoop.util.common.TBStringJonner;
import com.hadoop.util.common.exception.WrappedException;
import com.hadoop.util.common.fileUtil.FileUtil;
import jodd.util.StringUtil;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Repository;

import java.io.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Suntree.xu on 2016-8-17.
 */
@Repository
public class ExcelHandler {
    public static void main(String[] args){
        ExcelHandler excelHandler = new ExcelHandler();
        short builtinFormat = HSSFDataFormat
                .getBuiltinFormat("h:mm");
        short builtinFormat1 = HSSFDataFormat
                .getBuiltinFormat("yyyy-m-d");
        short builtinFormat2 = HSSFDataFormat
                .getBuiltinFormat("yyyy-mm-dd");

        XSSFWorkbook xssfworkbook = new XSSFWorkbook();
//        xssfworkbook =(XSSFWorkbook) write2007Excel("e:/aa.xlsx");
        XSSFSheet sheet = xssfworkbook.createSheet("add pic");
        try {
            InputStream file = new FileInputStream("e:/abc.jpg");
            byte[] data = new byte[file.available()];
            file.read(data);
            addPicture(xssfworkbook,0,data);
            FileUtil.createPath("e:/aa/");

            OutputStream outputStream = new FileOutputStream("e:/aa/pic.xlsx");
            xssfworkbook.write(outputStream);

        } catch (Exception e) {
            e.printStackTrace();
        }
        // excelHandler.getExcelMeta("D:\\xsx\\Competence+\\query_result_25.xls");
        //List<String> result = excelHandler.getDatafromExcel("D:\\xsx\\Competence+\\query_result_25.xls");
        //System.out.println(result.size());

    }

    /**
     * 取得表头信息
     * @param excelUrl
     * @return
     */
    public List<String> getExcelMeta(String excelUrl){
        List<String> result = new ArrayList<String>();
        try {
            InputStream input = new FileInputStream(excelUrl);  //建立输入流
            Workbook wb  = null;
            wb = new HSSFWorkbook(input);
            Sheet sheet = wb.getSheetAt(0);     //获得第一个表单
            Iterator<Row> rows = sheet.rowIterator(); //获得第一个表单的迭代器
            Row metaRow = sheet.getRow(1);//获得首行表头信息
            Iterator<Cell> cells = metaRow.cellIterator();    //获得第一行的迭代器
            while (cells.hasNext()) {
                Cell cell = cells.next();
                result.add(cell.getStringCellValue()==null?cell.getStringCellValue():cell.getStringCellValue());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    /**
     * 取得表头信息
     * @param excelUrl
     * @return
     */
    public List<String> getExcelMeta(String excelUrl,String sheetname,int startLineNum){
        List<String> result = new ArrayList<String>();
        try {
            InputStream input = new FileInputStream(excelUrl);  //建立输入流
            Workbook wb  = null;
            wb = new HSSFWorkbook(input);
            Sheet sheet = wb.getSheet(sheetname);     //获得表单
            Iterator<Row> rows = sheet.rowIterator(); //获得表单的迭代器
            Row metaRow = sheet.getRow(startLineNum==0?sheet.getFirstRowNum():startLineNum);//获得首行表头信息
            Iterator<Cell> cells = metaRow.cellIterator();    //获得第一行的迭代器
            while (cells.hasNext()) {
                Cell cell = cells.next();
                result.add(cell.getStringCellValue());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new WrappedException(" 解析头：Excel解析失败");

        }
        return result;
    }
    /**
     * 取得表头信息
     * @param excelUrl
     * @return
     */
    public List<String> get2007ExcelMeta(String excelUrl,String sheetname,int startLineNum){
        List<String> result = new ArrayList<String>();
        try {
            InputStream input = new FileInputStream(excelUrl);  //建立输入流
            Workbook wb = new XSSFWorkbook(input);
            Sheet sheet = wb.getSheet(sheetname);     //获得表单
            Iterator<Row> rows = sheet.rowIterator(); //获得表单的迭代器
            Row metaRow = sheet.getRow(startLineNum==0?sheet.getFirstRowNum():startLineNum);//获得首行表头信息
            Iterator<Cell> cells = metaRow.cellIterator();    //获得第一行的迭代器
            while (cells.hasNext()) {
                Cell cell = cells.next();
                result.add(getValue(cell));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new WrappedException(" 解析头：Excel解析失败");
        }
        return result;
    }


    public List<String> getDatafromExcel(String excelUrl){
        List<String> result = new ArrayList<String>();
        try {
            InputStream input = new FileInputStream(excelUrl);  //建立输入流
            Workbook wb  = null;
            wb = new HSSFWorkbook(input);
            Sheet sheet = wb.getSheetAt(0);     //获得第一个表单

            Iterator<Row> rows = sheet.rowIterator(); //获得第一个表单的迭代器
            while (rows.hasNext()) {
                Row row = rows.next();  //获得行数据
                if(row.getRowNum() < 2){
                    continue;
                }
                Iterator<Cell> cells = row.cellIterator();    //获得第一行的迭代器
                String resultTmp ="";
                while (cells.hasNext()) {
                    Cell cell = cells.next();
                   resultTmp +=getValue(cell)+",";
                }
                resultTmp = resultTmp.substring(0,resultTmp.length()-1);
                result.add(resultTmp);
            }
        } catch (Exception ex) {
            throw new WrappedException(" Excel解析失败");
        }
        return result;
    }

    /**
     *  去掉excel表头  根据表单名和开始行数 获取存数据
     * @param excelUrl
     * @param sheetName
     * @param startLineNum
     * @return
     */
    public List<String> getDatafromExcel(String excelUrl, String sheetName, int startLineNum, String split, int endNum){
        try {
            InputStream input = new FileInputStream(excelUrl);
            //创建excel文件对象
            Workbook wb = new HSSFWorkbook(input);
            return  getDatafromExcel( wb, sheetName, startLineNum,split,endNum);
        } catch (Exception e) {
            throw new WrappedException(" Excel解析失败");
        }
    }
    /**
     *  去掉excel表头  根据表单名和开始行数 获取存数据
     * @param excelUrl
     * @param sheetName
     * @param startLineNum
     * @return
     */
    public List<String> getDatafrom2007Excel(String excelUrl,String sheetName,int startLineNum,String split,int endNum){
        try {
            InputStream input = new FileInputStream(excelUrl);
            //创建excel文件对象
            Workbook wb = new XSSFWorkbook(input);
            return getDatafromExcel( wb, sheetName, startLineNum,split,endNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null ;
    }
    public List<String> getDatafromExcel(Workbook wb, String sheetName, int startLineNum,String split,int endNum){
        List<String> result = new ArrayList<String>();
        try {
            Sheet sheet = StringUtil.isBlank(sheetName)?wb.getSheetAt(0):wb.getSheet(sheetName);     //根据表单名获得表单
            startLineNum = startLineNum==0?sheet.getFirstRowNum():startLineNum;
            Iterator<Row> rows = sheet.rowIterator(); //获得表单的迭代器
            while (rows.hasNext()) {
                Row row = rows.next();  //获得行数据
                if(row.getRowNum() < (startLineNum)){
                    continue;
                }
                if (endNum>0&&row.getRowNum()>endNum) break;
                short firstCellNum = row.getFirstCellNum();
                short lastCellNum = row.getLastCellNum();
//                System.out.println(firstCellNum +" "+lastCellNum);
                String resultTmp ="";
                for (int i = firstCellNum;i< lastCellNum;i++){
                    Cell cell = row.getCell(i);
                    if (cell ==null){
                        //todo 标记有空列
                        resultTmp += split;
                        continue;
                    }
                    resultTmp +=getValue(cell)+split;
                }
                resultTmp = resultTmp.substring(0,resultTmp.length()-1);

                result.add(resultTmp);
            }
        } catch (Exception ex) {
            throw new WrappedException(" Excel解析失败");
        }
        return result;
    }
    /**
     *  去掉excel表头  根据表单名和开始行数 获取存数据
     * @param excelUrl
     * @param sheetName
     * @param startLineNum
     * @return
     */
    public List<Integer> getDataTypefromExcel(String excelUrl,String sheetName,int startLineNum,int length,int endNum){
        try {
            InputStream input = new FileInputStream(excelUrl);
            //创建excel文件对象
            Workbook wb = new HSSFWorkbook(input);
            return getDataTypefromExcel(wb,sheetName,startLineNum,length,endNum);
        } catch (Exception e) {
            throw new WrappedException(" Excel解析失败");
        }
    }
    /**
     *  去掉excel表头  根据表单名和开始行数 获取存数据
     * @param excelUrl
     * @param sheetName
     * @param startLineNum
     * @return
     */
    public List<Integer> getDataTypefrom2007Excel(String excelUrl,String sheetName,int startLineNum,int length,int endNum){
        try {
            InputStream input = new FileInputStream(excelUrl);
            //创建excel文件对象
            Workbook wb = new XSSFWorkbook(input);
            return  getDataTypefromExcel(wb,sheetName,startLineNum,length,endNum);
        } catch (Exception e) {
            throw new WrappedException(" Excel解析失败");
        }
    }

    public List<Integer> getDataTypefromExcel(Workbook wb,String sheetName,int startLineNum,int length,int endNum){

        List<Integer> result = new ArrayList<Integer>();
        try {

            //创建一个张表
            Sheet sheet = StringUtil.isBlank(sheetName)?wb.getSheetAt(0):wb.getSheet(sheetName);     //根据表单名获得表单
            Iterator<Row> rows = sheet.rowIterator(); //获得表单的迭代器
            length = length==0?100:length ;
            while (rows.hasNext()) {
                Row row = rows.next();  //获得行数据
                if ((endNum>0&&row.getRowNum()>endNum)||row.getRowNum()>startLineNum+length) break;
                if(row.getRowNum() < (startLineNum+1)){
                    continue;
                }
                int k = 0 ;
                for (int i =row.getFirstCellNum() ;i<row.getLastCellNum();i++){
                    k++ ;
                    Cell cell = row.getCell(i);
                    if (cell==null){
                        if (row.getRowNum()==startLineNum+1){
                            result.add(null);
                        }
                        continue;
                    }
                    int type = DBType.getDBType(getValue(cell));
                    if (row.getRowNum()>startLineNum+1){
                        Integer preType = result.get(k-1);
                        if ( preType == null||type>preType) {
                            result.set(k-1,type);
                        }
                    }else {
                        result.add(type) ;
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }
    private String getValue(Cell hssfCell) {
        if (hssfCell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
            // 返回布尔类型的值
            return String.valueOf(hssfCell.getBooleanCellValue());
        } else if (hssfCell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
            // 返回数值类型的值
            String result = null ;
            if (HSSFDateUtil.isCellDateFormatted(hssfCell)) {// 处理日期格式、时间格式
                SimpleDateFormat sdf = null;
//                System.out.println(hssfCell.getCellStyle().getDataFormat());
                if (hssfCell.getCellStyle().getDataFormat() == HSSFDataFormat
                        .getBuiltinFormat("h:mm")) {
                    sdf = new SimpleDateFormat("HH:mm");
                } else  {// 日期
                    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                }
                Date date = hssfCell.getDateCellValue();
                result = sdf.format(date);
            } else if (hssfCell.getCellStyle().getDataFormat() == 58) {
                // 处理自定义日期格式：m月d日(通过判断单元格的格式id解决，id的值是58)
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                double value = hssfCell.getNumericCellValue();
                Date date = org.apache.poi.ss.usermodel.DateUtil
                        .getJavaDate(value);
                result = sdf.format(date);
            } else {
                double value = hssfCell.getNumericCellValue();
                CellStyle style = hssfCell.getCellStyle();
                DecimalFormat format = new DecimalFormat();
                String temp = style.getDataFormatString();
                // 单元格设置成常规
                if (temp.equals("General")) {
                    format.applyPattern("#");
                }
                result = format.format(value);
            }
            return result;
        } else {
            // 返回字符串类型的值
            return String.valueOf(hssfCell.getStringCellValue());
        }
    }

   public void writeData2Excel(List<Map<String,String>> resultData,String exportUrl,String tableName){
        //工作簿
        HSSFWorkbook hssfworkbook = new HSSFWorkbook();
        //创建sheet页
        HSSFSheet hssfsheet = hssfworkbook.createSheet();
        //sheet名称乱码处理
        hssfworkbook.setSheetName(0,tableName);
        Font font1 = createFonts(hssfworkbook, Font.BOLDWEIGHT_NORMAL, "宋体", false,
                (short) 200);
        HSSFRow hssfrow;
       HSSFRow hssfrow_0 = hssfsheet.createRow(0);
        int k = 0;
        if(resultData.size()>0) {
            for (String key : resultData.get(0).keySet()) {
                createCell(hssfworkbook, hssfrow_0, k,key.split(".")[1], font1);
                k++;
            }
        }
        for(int i = 0;i < resultData.size();i ++){
            hssfrow = hssfsheet.createRow(i+1);
            int j = 0;
            for(String key:resultData.get(i).keySet()){
                createCell(hssfworkbook, hssfrow, j, resultData.get(i).get(key), font1);
                j ++;
            }
        }
        //输出
        try {
            FileOutputStream fileoutputstream = new FileOutputStream(exportUrl);
            hssfworkbook.write(fileoutputstream);
            fileoutputstream.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /*
    *  将数据导入到excel中。格式跟数据库中数据格式一样
     */
    public static  XSSFWorkbook exportDataFromHive2Excel(List<List> resultData,String sheetName,int sheetIndex ){
        //工作簿
        XSSFWorkbook hssfworkbook = new XSSFWorkbook();
        exportDataFromHive2Excel(hssfworkbook,resultData,sheetName,sheetIndex);
        return hssfworkbook ;
    }
    public static  XSSFWorkbook exportDataFromHive2Excel(XSSFWorkbook xssfWorkbook, List<List> resultData,String sheetName,int sheetIndex ){
        //创建sheet页
        Sheet hssfsheet = xssfWorkbook.createSheet();
        //sheet名称乱码处理
        xssfWorkbook.setSheetName(sheetIndex,sheetName);
        Font font1 = createFonts(xssfWorkbook, Font.BOLDWEIGHT_NORMAL, "宋体", false,
                (short) 200);
        Row hssfrow;
        Row row_0 = hssfsheet.createRow(0);
        List<String> titles = resultData.get(0);
        List<List<String>> datas = resultData.get(1);
        int k = 0;
        if(titles.size()>0) {
            for (String key : titles) {
                createCell(xssfWorkbook, row_0, k, key, font1);
                k++;
            }
        }
        for(int i = 0;i < datas.size();i ++){
            hssfrow = hssfsheet.createRow(i+1);
            int j = 0;
            for(String key:datas.get(i)){
                createCell(xssfWorkbook, hssfrow, j, key, font1);
                j ++;
            }
        }

        return xssfWorkbook ;
    }


    public static Row getRow(Sheet sheet,int index){
        Row row = sheet.getRow(index);
        if (row==null){
            row = sheet.createRow(index);
        }
        return row ;
    }
    public static void addPicture(XSSFWorkbook xssfWorkbook,int sheetIndex ,byte[] bytes){
        Sheet sheetAt = xssfWorkbook.getSheetAt(sheetIndex);
        //画图的顶级管理器，一个sheet只能获取一个（一定要注意这点）
        Drawing drawingPatriarch = sheetAt.createDrawingPatriarch();
        //创建锚点
        ClientAnchor clientAnchor = drawingPatriarch.createAnchor(0, 0, 1023, 100, (short) 1, 1, (short) 12, 15);
        clientAnchor.setAnchorType(0);
        int i = xssfWorkbook.addPicture(bytes, XSSFWorkbook.PICTURE_TYPE_BMP);
        drawingPatriarch.createPicture(clientAnchor,i);

    }

    public static void createCell(Row row,int column,String value,boolean isConvert){
        int physicalNumberOfCells = row.getPhysicalNumberOfCells();
        Cell cell ;
        if (physicalNumberOfCells>column){
             cell = row.getCell(column);
                if (cell.getCellType()==Cell.CELL_TYPE_NUMERIC){
                    if (cell.getNumericCellValue()==0){
                        if (isConvert){
                            int dbType = DBType.getDBType(value);
                            if (dbType==DBType.Int_TYPE||dbType==DBType.BigInt_TYPE){
                                cell.setCellValue(new Long(value));
                            }else if (dbType==DBType.Double_TYPE){
                                cell.setCellValue(new Double(value));
                            }else {
                                cell.setCellValue(value);
                            }
                        }else {
                            cell.setCellValue(value);
                        }
                    }
                }
        }else {
             cell = row.createCell(column);
            if (isConvert){
                int dbType = DBType.getDBType(value);
                if (dbType==DBType.Int_TYPE||dbType==DBType.BigInt_TYPE){
                    cell.setCellValue(new Long(value));
                }else if (dbType==DBType.Double_TYPE){
                    cell.setCellValue(new Double(value));
                }else {
                    cell.setCellValue(value);
                }
            }else {
                cell.setCellValue(value);
            }
        }



    }
    /**
     * 创建单元格并设置样式,值
     *
     * @param wb
     * @param row
     * @param column
     * @param
     * @param
     * @param value
     */
    public static void createCell(Workbook wb, Row row, int column,
                                  String value, Font font) {
        createCell(row,column,value,true);
       // CellStyle cellStyle = wb.createCellStyle();
        //cellStyle.setAlignment(XSSFCellStyle.ALIGN_CENTER);
        //cellStyle.setVerticalAlignment(XSSFCellStyle.VERTICAL_BOTTOM);
       // cellStyle.setFont(font);
       // cell.setCellStyle(cellStyle);
    }
    /**
     * 设置字体
     *
     * @param wb
     * @return
     */
    public static Font createFonts(Workbook wb, short bold, String fontName,
                                   boolean isItalic, short hight) {
        Font font = wb.createFont();
        font.setFontName(fontName);
        font.setBoldweight(bold);
        font.setItalic(isItalic);
        font.setFontHeight(hight);
        return font;
    }

    public static Workbook write2007Excel(String filePath) {
        //创建excel文件对象
        Workbook wb = null ;
        try {
            InputStream input = new FileInputStream(filePath);
            //创建excel文件对象
            wb = new XSSFWorkbook(input);
            return wb;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static XSSFWorkbook write2007Excel(InputStream inputStream) {
        //创建excel文件对象
        XSSFWorkbook wb = null ;
        try {
            //创建excel文件对象
            wb = new XSSFWorkbook(inputStream);
            return wb;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static Workbook write2003Excel(String filePath) {
        //创建excel文件对象
        Workbook wb = null ;
        try {
            InputStream input = new FileInputStream(filePath);
            //创建excel文件对象
            wb = new HSSFWorkbook(input);
            return wb;
        } catch (Exception e) {
            throw new WrappedException(" WorkBook创建失败");

        }

    }
    public static List<String> getSheetNamesFromExcel(Workbook wb){
        List<String> list = new ArrayList<>() ;
        int numberOfSheets = wb.getNumberOfSheets();
        for (int i =0;i<numberOfSheets;i++){
            Sheet sheetAt = wb.getSheetAt(i);
            if(sheetAt.getPhysicalNumberOfRows()>0) {
                list.add(sheetAt.getSheetName());
            }
        }
        return list ;
    }

    public static List<String> getSheetNamesFromExcel(String filePath){
        Workbook wb = null ;
        InputStream input = null;
        try {
            input = new FileInputStream(filePath);
            //创建excel文件对象
            if (FileUtil.getFileType(filePath).toLowerCase().equals("xls")){
                wb = new HSSFWorkbook(input);
            }else if(FileUtil.getFileType(filePath).toLowerCase().equals("xlsx")){
                wb = new XSSFWorkbook(input);
            }
            if(wb==null) return  null ;
            return getSheetNamesFromExcel(wb) ;
        } catch (Exception e) {
            throw new WrappedException(" Excel解析失败");
        }finally {
            try {
                input.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
