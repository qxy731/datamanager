package com.myboot.dataprocess.model;


import io.swagger.annotations.ApiModel;
import lombok.Data;

/**
 * HBase的进件流水单
 */
@Data
@ApiModel(value = "HBase的进件流水单", description = "HBase的进件流水单")
public class HBaseApplyCardEntity {
	
	private String rowkey;
	
	private String columnFamily;
    
}