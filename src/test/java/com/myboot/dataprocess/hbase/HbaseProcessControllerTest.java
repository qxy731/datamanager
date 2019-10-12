package com.myboot.dataprocess.hbase;

import java.net.URL;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import com.myboot.dataprocess.DataprocessApplication;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = DataprocessApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class HbaseProcessControllerTest {
	
	/*@Autowired
    private WebApplicationContext context;*/

/*    @Autowired
    private BookRepository bookRepository;*/

	@LocalServerPort
	private int port;

    //private MockMvc mockMvc;
    
    private URL base;

    @Autowired
    private TestRestTemplate restTemplate;

    /*@Before
    public void setupMockMvc() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }*/
    
    @Before
    public void setUp() throws Exception {
        String url = String.format("http://localhost:%d/", port);
        System.out.println(String.format("port is : [%d]", port));
        this.base = new URL(url);
    }
    
    /**
     * 向"/hbase/save"地址发送请求，并打印返回结果
     * @throws Exception
     */
    @Test
    public void testSave() throws Exception {
    	//同HbaseProcessServiceImplTest#testSave()方法，请先测试HbaseProcessServiceImplTest#testSave()
    	int total = 100;
		String startDate = "2019-09-01";
		String endDate = "2019-10-07";
		String url = this.base.toString() + "/dataprocess/hbase/save?total="+total+"&startDate="+startDate+"&endDate="+endDate;
        ResponseEntity<String> response = this.restTemplate.getForEntity(url, String.class, "");
        System.out.println(String.format("测试结果为：%s", response.getBody()));
    }
}