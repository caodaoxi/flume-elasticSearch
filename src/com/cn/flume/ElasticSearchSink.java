package com.cn.flume;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
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

public class ElasticSearchSink extends EventSink.Base {
    private String clusterName = "elasticsearch";
	private String indexName = null;
	private String dataType = null;
	private String host = "localhost";
	private String delimiter = "\t";
	private Charset charset = Charset.defaultCharset();
	private int port = 9300;
	private SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd");
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
		XContentBuilder builder = jsonBuilder();
		
		builder.startObject().field("@message", message);
		builder.field("@timestamp", new Date(e.getTimestamp()));
		builder.field("@source", "file://" + this.host + "/var/log/pv.log");
		builder.field("@source_host", e.getHost());
		builder.field("@src_path", "/var/log/pv.log");
		builder.field("@type", this.dataType);
		builder.startObject("@fields");
        for (Map.Entry<String, byte[]> entry : e.getAttrs().entrySet()) {
        	builder.field(entry.getKey(), new String(entry.getValue(), charset));
        }
        builder.endObject();
        builder.startArray("@tags");
        builder.endArray();
        builder.endObject();
		IndexResponse response = client.prepareIndex(this.indexName + "-" + df.format(new Date()), this.dataType)
		        .setSource(builder).execute().actionGet();
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

