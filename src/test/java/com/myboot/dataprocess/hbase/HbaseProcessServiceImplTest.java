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
		int total = 200;
		String startDate = "2019-04-01";
		String endDate = "2019-10-08";
		//service.save(total, startDate, endDate);
		//service.other();//第一步先测试通不通
		//System.out.println("--------------测试----------");
	}
	
	@Test
    public void testSelectAll() throws Exception {
		service.process();
		//service.other();//第一步先测试通不通
		//System.out.println("--------------测试----------");
	}

}
