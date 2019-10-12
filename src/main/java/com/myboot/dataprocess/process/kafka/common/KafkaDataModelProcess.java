package com.myboot.dataprocess.process.kafka.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.myboot.dataprocess.builder.RandomDataModelBuilder;
import com.myboot.dataprocess.builder.RowkeyGenerator;
import com.myboot.dataprocess.model.ApplyCardEntity;
import com.myboot.dataprocess.model.KafkaApplyCardEntity;
import com.myboot.dataprocess.model.ProtocolEntity;
import com.myboot.dataprocess.model.SchemaEntity;

@Component
public class KafkaDataModelProcess {
	
	@Autowired
	private MyKafkaConfiguration myKafkaConfiguration;
	
	public KafkaApplyCardEntity assembleKafkaData(String currentDate) {
		KafkaApplyCardEntity apply  = new KafkaApplyCardEntity();
		ProtocolEntity protocol = new ProtocolEntity();
		protocol.setType(myKafkaConfiguration.getOtherParameter("protocol.type"));
		protocol.setVersion(myKafkaConfiguration.getOtherParameter("protocol.version"));
		SchemaEntity schema = new SchemaEntity();
		schema.setNamespace(myKafkaConfiguration.getOtherParameter("source.topic"));
		schema.setTableName(myKafkaConfiguration.getOtherParameter("phoenix_table_name"));
		apply.setProtocol(protocol);
		apply.setSchema(schema);
		apply.setTimestamp(System.currentTimeMillis());
		RowkeyGenerator generator = RowkeyGenerator.getInstance(currentDate);
		long sequence = generator.getSequence();
		ApplyCardEntity data = RandomDataModelBuilder.getRandomDataModel(sequence, currentDate);
		apply.setData(data);
		return apply;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
