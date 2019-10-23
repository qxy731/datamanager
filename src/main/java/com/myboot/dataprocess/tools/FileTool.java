package com.myboot.dataprocess.tools;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class FileTool {
	
	
	public static void writeFile(String filePath,String lineContent) throws IOException {
		//读取文件(字节流)
        //Reader in = new InputStreamReader(new FileInputStream("d:\\1.txt"),"UTF8");
        //写入相应的文件
        PrintWriter out = new PrintWriter(new FileWriter(filePath));
        //读取数据
        //循环取出数据
        /*byte[] bytes = new byte[1024];
        int len = -1;
        while ((len = in.read()) != -1) {
            System.out.println(len);
            //写入相关文件
            out.write(lineContent);
        }*/
        out.write(lineContent);
        //清楚缓存
        out.flush();
        //关闭流
        out.close();
	}

	public static void main(String[] args) {
		
	}

}
