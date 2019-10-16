package com.myboot.dataprocess.model;


import java.util.Map;

import io.swagger.annotations.ApiModel;
import lombok.Data;

/**
 * kafka的进件流水单
 */
@Data
@ApiModel(value = "kafka的进件流水单", description = "kafka的进件流水单")
public class KafkaApplyCardEntity {
	
	private ProtocolEntity protocol;
	
	private SchemaEntity schema;
	
	private long timestamp;
	
	private Map<String,Object> data;

}