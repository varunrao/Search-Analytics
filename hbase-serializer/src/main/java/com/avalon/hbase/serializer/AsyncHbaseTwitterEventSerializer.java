package com.avalon.hbase.serializer;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

public class AsyncHbaseTwitterEventSerializer implements
		AsyncHbaseEventSerializer {
	private byte[] table;
	private byte[] colFam;
	private Event currentEvent;
	private byte[][] columnNames;
	private final List<PutRequest> puts = new ArrayList<PutRequest>();
	private final List<AtomicIncrementRequest> incs = new ArrayList<AtomicIncrementRequest>();
	private byte[] currentRowKey;
	private final byte[] eventCountCol = "eventCount".getBytes();

	@Override
	public void initialize(byte[] table, byte[] cf) {
		this.table = table;
		this.colFam = cf;
	}

	@Override
	public void setEvent(Event event) {
		// Set the event and verify that the rowKey is not present
		this.currentEvent = event;
	}

	@Override
	public List<PutRequest> getActions() {
		// Split the event body and get the values for the columns
		String eventStr = new String(currentEvent.getBody());
		puts.clear();
		try {

			Status tweet = DataObjectFactory.createStatus(eventStr);
			currentRowKey = String.valueOf(tweet.getId()).getBytes();
			byte[] bCol;
			byte[] bVal;

			bCol = "text".getBytes();
			bVal = tweet.getText().getBytes();
			PutRequest req = new PutRequest(table, currentRowKey, colFam, bCol,
					bVal);
			puts.add(req);

			bCol = "created_at".getBytes();
			bVal = String
					.valueOf(new Timestamp(tweet.getCreatedAt().getTime()))
					.getBytes();
			req = new PutRequest(table, currentRowKey, colFam, bCol, bVal);
			puts.add(req);
		} catch (TwitterException ex) {
			throw new RuntimeException("Error parsing Twitter string");
		}
		return puts;
	}

	@Override
	public List<AtomicIncrementRequest> getIncrements() {
		incs.clear();
		// Increment the number of events received
//		incs.add(new AtomicIncrementRequest(table, "totalEvents".getBytes(),
//				colFam, eventCountCol));
		return incs;
	}

	@Override
	public void cleanUp() {
		table = null;
		colFam = null;
		currentEvent = null;
		columnNames = null;
		currentRowKey = null;
	}

	@Override
	public void configure(Context context) {
		// Get the column names from the configuration
		String cols = new String(context.getString("columns", ""));

		if (cols.equals("")) {
			throw new FlumeException("Config columns is null");
		}

		String[] names = cols.split(",");
		columnNames = new byte[names.length][];
		int i = 0;
		for (String name : names) {
			columnNames[i++] = name.getBytes();
		}
	}

	@Override
	public void configure(ComponentConfiguration conf) {
	}
}