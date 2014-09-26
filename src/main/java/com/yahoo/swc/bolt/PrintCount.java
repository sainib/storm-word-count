package com.yahoo.swc.bolt;

import java.util.Map;
import java.io.*;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PrintCount extends BaseRichBolt {

	/**
	 * Bolt to print the current count to the console
	 */
	private static final long serialVersionUID = -6133323709164892042L;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// do nothing

	}

	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		Object count = tuple.getValue(1);
		String tweetStatus = tuple.getString(2);

		try{
            File file = new File("/tmp/hwx.txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(word + "\n");
            bw.close();			
		}catch(Exception e){
			//do nothing
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// do nothing
	}

}
