package com.cn.flume;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class Test {

	public static void main(String[] args) throws Exception{
			Settings settings = ImmutableSettings.settingsBuilder()
			        .put("cluster.name", "kuxun").build();
			Client client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
			XContentBuilder builder = jsonBuilder();
			builder.startObject().field("@message", "my name is caodaoxiewerwef");
			builder.field("@timestamp", new Date());
			builder.field("@source", "pp");
			builder.field("@source_host", "192.168.1.102");
			builder.field("@src_path", "/var/log/pv.log");
			builder.field("@type", "pvlolog");
			builder.startObject("@fields");
			builder.endObject();
			
	        builder.startArray("@tags");
	        builder.endArray();
	        builder.endObject();
	        
			IndexResponse response = client.prepareIndex("logstash-2013.09.15", "tms_jboss_syslog")
			        .setSource(builder).execute().actionGet();
			System.out.println(builder.string());
			client.close();
	}

}
