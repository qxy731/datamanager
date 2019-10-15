package com.myboot.dataprocess.hbase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.myboot.dataprocess.DataprocessApplication;
import com.myboot.dataprocess.process.hbase.MyHbaseProcessServiceImpl;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = DataprocessApplication.class)
public class HbaseProcessServiceImplTest {
	
	@Autowired
    private MyHbaseProcessServiceImpl service;
	
	@Test
    public void testSave() throws Exception {
		int total = 1;
		String startDate = "2019-10-08";
		String endDate = "2019-10-08";
		service.save(total, startDate, endDate);
		//System.out.println("--------------测试----------");
	}
	
}