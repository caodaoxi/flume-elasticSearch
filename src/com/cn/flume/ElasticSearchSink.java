package com.cn.flume;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;

public class ElasticSearchSink extends EventSink.Base {
    private String clusterName = "elasticsearch";
	private String indexName = null;
	private String dataType = null;
	private String host = "localhost";
	private String delimiter = "\t";
	private int port = 9300;
	private Client client = null;
	public ElasticSearchSink(String clusterName, String indexName, String dataType, String host, int port, String delimiter) {
		this.clusterName = clusterName;
		this.indexName = indexName;
		this.host = host;
		this.port = port;
		this.dataType = dataType;
		this.delimiter = delimiter;
	}

	@Override
	public void open() throws IOException {
		Settings settings = ImmutableSettings.settingsBuilder()
		        .put("cluster.name", this.clusterName).build();
		this.client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(this.host, this.port));
	}

	@Override
	public void append(Event e) throws IOException {
		String message = new String(e.getBody());
		IndexResponse response = client.prepareIndex(this.indexName, this.dataType, "1")
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("host", e.getHost())
		                        .field("timestamp", e.getTimestamp())
		                        .field("message", message)
		                        .field("type", this.dataType)
		                    .endObject()
		                  )
		        .execute()
		        .actionGet();
		//String[] fields = message.split(this.delimiter);
	}

	@Override
	public void close() throws IOException {
		this.client.close();
	}

	public static SinkBuilder builder() {
		return new SinkBuilder() {
			@Override
			public EventSink build(Context context, String... argv) {
				return new ElasticSearchSink(argv[0], argv[1], argv[2], argv[3] ,Integer.parseInt(argv[4]), argv[5]);
			}
		};
	}

	/**
	 * This is a special function used by the SourceFactory to pull in this
	 * class as a plugin sink.
	 */
	public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
		List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
		builders.add(new Pair<String, SinkBuilder>("elasticSearch", builder()));
		return builders;
	}
}

