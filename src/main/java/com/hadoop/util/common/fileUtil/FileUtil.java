package com.hadoop.util.common.fileUtil;



import com.hadoop.util.common.DBType;
import com.hadoop.util.common.exception.BizEnums;
import com.hadoop.util.common.exception.WrappedException;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by luosai on 2017/5/12.
 */
public class FileUtil {

    public static void main(String[] args) {

        FileUtil fileUtil = new FileUtil();
//        List list = fileUtil.readOneLineFromFile("F:\\test.txt",2,",");
//        List list = fileUtil.readNLineFromFile("F:\\test.txt",2,null,null);
//        System.out.println(list);
    }

    /**
     * 读取文件中所有内容
     * @param fileName
     * @return
     * @throws IOException
     */
    public List<String> readFile(String fileName) throws IOException {
        List<String> strings = new ArrayList<String>() ;
        FileInputStream fis = new FileInputStream(fileName);
        InputStreamReader isr=new InputStreamReader(fis, "UTF-8");
        LineNumberReader reader = new LineNumberReader(isr);
        String line = "";
        while ((line=reader.readLine())!=null) {
            strings.add(line);
        }
        reader.close();
        isr.close();
        fis.close();
        return strings ;
    }

    /**
     * 从第n行开始读取 到文件结尾
     * @param fileName
     * @param lineNum
     * @param splits
     * @param charsetName
     * @param types
     * @param
     * @param
     * @return
     */
    public static List<String> readNLineFromFile(String fileName, int lineNum, String splits, String charsetName, List<Integer>types , int columnSize, int endNum) {
        if (lineNum>getTotalLines(fileName)||lineNum<=0) return null ;
        List<String> strings = new ArrayList<>() ;
        try {
            FileInputStream fis = new FileInputStream(fileName);
            InputStreamReader isr=new InputStreamReader(fis, charsetName!=null?charsetName:"UTF-8");
            LineNumberReader reader = new LineNumberReader(isr);
            String line = "";
            StringBuilder stringBuilder = new StringBuilder() ;
            while ((line=reader.readLine())!=null) {
                line=line.trim();
                if (reader.getLineNumber()<lineNum||line.equals("")) continue;
                if (endNum!=0 &&reader.getLineNumber()>endNum ) break;
                if (types!=null){
                    String split = splits.equals("|")?"\\|":splits;
                    String[] arr = line.split(split==null?",":split);
                    if (arr.length<columnSize){
                        throw new WrappedException(BizEnums.FILE_COLUMN_NOT_EQUAL,"第"+reader.getLineNumber()+"行的列数为"+arr.length +"小于头部的列"+columnSize+"\n");
                    }else if (arr.length>columnSize){
                        throw new WrappedException(BizEnums.FILE_COLUMN_NOT_EQUAL,"第"+reader.getLineNumber()+"行的列数为"+arr.length +"大于头部的列"+columnSize+"\n");
                    }
                    if (Arrays.asList(split).contains("")){
                        //todo 如果为空字符串 则调用处理函数 补上数据
                        arr = line.split(split==null?",":split);
                    }
                    StringBuilder builder = new StringBuilder();
                    for (int i = 0 ;i<arr.length;i++){
                        if (types.get(i)== DBType.Date_TYPE||types.get(i)== DBType.TimeStamp_TYPE){
                            builder.append(arr[i].trim().replace("/","-")).append(splits);
                        }else {
                            builder.append(arr[i].trim()).append(splits);
                        }
                    }
                    builder.deleteCharAt(builder.length()-1);
                    line = builder.toString();
                    strings.add(line) ;
                }else {
                    strings.add(line);
                }

            }
            reader.close();
            isr.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return strings ;
    }
    /**
     * 从第n行开始读取 到文件结尾
     * @param fileName
     * @param startNum
     * @param splits
     * @param charsetName
     * @return
     */
    public static List<Integer> readDataType(String fileName, int startNum,int length,String splits,String charsetName,int columnSize,int endNum) {
        if (startNum>getTotalLines(fileName)||startNum<0) return null ;
        List<Integer> list = new ArrayList<>() ;
        try {
            FileInputStream fis = new FileInputStream(fileName);
            InputStreamReader isr=new InputStreamReader(fis, charsetName!=null?charsetName:"UTF-8");
            LineNumberReader reader = new LineNumberReader(isr);

            String line = "";
            length = length==0?100:length ;
            while ((line=reader.readLine())!=null) {
                if (reader.getLineNumber() <startNum||line.equals("")) continue;
                if ((endNum!=0 &&reader.getLineNumber()>endNum) ||reader.getLineNumber()>startNum+length) break;
                line=line.trim();
                splits = splits.equals("|")?"\\|":splits;

                if (splits ==null) {
                    list.add(DBType.getDBType(line.trim()));
                }else {

                    String[] split = line.split(splits);
                    if (split.length<columnSize){
                        throw new WrappedException(BizEnums.FILE_COLUMN_NOT_EQUAL,"第"+reader.getLineNumber()+"行的列数小于表头的的列,请重新上传");
                    }else if (split.length>columnSize){
                        throw new WrappedException(BizEnums.FILE_COLUMN_NOT_EQUAL,"第"+reader.getLineNumber()+"行的列数大于表头的的列,多余的列将不会导入");
                    }
                    for (int i = 0 ;i<split.length;i++){
                        if (list.size()==split.length){
                            Integer preType = list.get(i);
                            //如果以前某列类型为空，最后一行的某列值为空，为默认类型string
                            if (split[i].trim().equals("")) {
                                if (preType ==null&&reader.getLineNumber()==getTotalLines(fileName)){
                                    list.set(i,DBType.getDBType(split[i].trim()));
                                }
                            }else {
                                int type = DBType.getDBType(split[i].trim()) ;
                                if (preType==null||type>preType) list.set(i,type) ;
                            }

                        }else {
                            if (split[i].trim().equals("")){
                                list.add(null);
                            }else {
                                list.add(DBType.getDBType(split[i].trim()));

                            }
                        }
                    }
                }
            }
            reader.close();
            isr.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return list ;
    }
    /**
     * 读取第n行
     * @param fileName
     * @param lineNum
     * @param split
     * @return
     * @throws IOException
     */
    public static List<String> readOneLineFromFile(String fileName, int lineNum,String split,String charsetName)  {
        if (lineNum>getTotalLines(fileName)||lineNum<0) return null ;
        List<String> list = null ;
        try {
            FileInputStream fis = new FileInputStream(fileName);
            InputStreamReader isr=new InputStreamReader(fis, charsetName!=null?charsetName:"UTF-8");
            LineNumberReader reader = new LineNumberReader(isr);
            String line="";
            String[] arrs=null;
            split = split.equals("|")?"\\|":split;

            while ((line=reader.readLine())!=null) {
                if (reader.getLineNumber()!=lineNum) continue;
                line=line.trim();
                if (!line.equals("")){
                    arrs= split==null?line.split(","):line.split(split);
                    list = Arrays.asList(arrs);
                    list.forEach(a->a.trim());
                    break;
                }
            }
            reader.close();
            isr.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return list ;
    }

    public static  int getTotalLines(String filename) {
        FileReader in = null;
        int lines = 0;
        try {
            in = new FileReader(filename);
            LineNumberReader reader = new LineNumberReader(in);
            String s  = reader.readLine();
            while (s != null) {
                lines++;
                s = reader.readLine();
            }
            reader.close();
            in.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return lines;
    }

    public static String getFileType(String fileName) {
        String[] strArray = fileName.split("\\.");
        int suffixIndex = strArray.length -1;
        return strArray[suffixIndex];
    }
    public static  void deleteFile(String fileName){
        File file = new File(fileName);
        if (file.exists()){
            file.delete();
        }
    }
    public static boolean createPath(String filePath){
        File file = new File(filePath);
        if (!file.exists()){
           return file.mkdirs();
        }
        return true ;
    }
}
