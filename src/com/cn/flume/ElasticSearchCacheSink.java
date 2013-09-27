package com.cn.flume;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;

public class ElasticSearchCacheSink extends EventSink.Base {
    private String clusterName = "elasticsearch";
	private String indexName = null;
	private InetSocketTransportAddress[] serverAddresses;
	private TimeCacheList<XContentBuilder> messageBuilders = null;
	private Charset charset = Charset.defaultCharset();
	
	private long rooltime = 1000;
	private int numBuckets = 4;
	private SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd");
	private Client client = null;
	public ElasticSearchCacheSink(String clusterName, String indexName, String host) {
		this.clusterName = clusterName;
		this.indexName = indexName;
		
		String[] hostNames = null;
	    hostNames = host.split(",");

	    serverAddresses = new InetSocketTransportAddress[hostNames.length];
	    for (int i = 0; i < hostNames.length; i++) {
	    	String[] hostPort = hostNames[i].split(":");
	    	String ht = hostPort[0];
	    	int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1]) : 9300;
	    	serverAddresses[i] = new InetSocketTransportAddress(ht, port);
	    }
	}

	@Override
	public void open() throws IOException {
		Settings settings = ImmutableSettings.settingsBuilder()
		        .put("cluster.name", this.clusterName).build();
		
		TransportClient transport = new TransportClient(settings);
		for (InetSocketTransportAddress host : serverAddresses) {
		      transport.addTransportAddress(host);
		}
		client = transport;
		messageBuilders = new TimeCacheList<XContentBuilder>(this.rooltime, this.numBuckets, new TimeCacheList.ExpiredCallback<XContentBuilder>(){

			@Override	
			public void expire(List<XContentBuilder> builders) {
				BulkRequestBuilder bulkRequest = client.prepareBulk();
				for(XContentBuilder builder : builders) {
					IndexRequestBuilder request = client.prepareIndex(ElasticSearchCacheSink.this.indexName + "-" + df.format(new Date()), "kibana").setSource(builder);
					bulkRequest.add(request);
				}
				BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				if (bulkResponse.hasFailures()) {
					System.out.println(bulkResponse.buildFailureMessage());
			    }
			}
			
		});
	}

	@Override
	public void append(Event e) throws IOException {
		String fileName = new String(e.get("tailSrcFile"));
		String message = new String(e.getBody());
		String host = e.getHost();
		String type = host + fileName;
		XContentBuilder builder = jsonBuilder();
		builder.startObject().field("@message", message);
		builder.field("@timestamp", new Date(e.getTimestamp()));
		builder.field("@source", fileName);
		builder.field("@source_host", host);
		builder.field("@src_path", fileName);
		builder.field("@type", type);
		builder.startObject("@fields");
        for (Map.Entry<String, byte[]> entry : e.getAttrs().entrySet()) {
        	builder.field(entry.getKey(), new String(entry.getValue(), charset));
        }
        
        builder.endObject();
        builder.startArray("@tags");
        builder.endArray();
        builder.endObject();
        
        messageBuilders.add(builder);
	}

	@Override
	public void close() throws IOException {
		this.client.close();
	}

	public static SinkBuilder builder() {
		return new SinkBuilder() {
			@Override
			public EventSink build(Context context, String... argv) {
				return new ElasticSearchCacheSink(argv[0], argv[1], argv[2]);
			}
		};
	}

	/**
	 * This is a special function used by the SourceFactory to pull in this
	 * class as a plugin sink.
	 */
	public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
		List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
		builders.add(new Pair<String, SinkBuilder>("elasticCacheSearch", builder()));
		return builders;
	}
}

