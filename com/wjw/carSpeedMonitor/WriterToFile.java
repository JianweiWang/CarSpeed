package com.wjw.carSpeedMonitor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WriterToFile implements IBasicBolt{
	static Date currentTime = new Date();
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
		
	}
	public void print_result(Tuple input) {
		
		FileWriter fw = null;
		//Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
			String time = formatter.format(currentTime);

			//System.out.println("prepare to print the statistics");
			try {
				fw = new FileWriter(new File("/root/result-carSpeedMonitor-" + time +".txt"),true);
				fw.write(input.getString(0)+"\t" + input.getInteger(1) + "\t" + input.getInteger(2) + "\n");
//				for(String key: counts.keySet()) {
////					System.out.println(key + "\t" +counts.get(key));
//				
//					fw.write(key + " " + counts.get(key).toString() + "\n");
//				}

				fw.close();

			} catch(IOException e) {
				e.printStackTrace();
			}
			//System.out.println("finish printing");
		
    }
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		if(!input.getString(0).equals("0000")) {
			System.out.println("writerfile receive: " + input.toString());
			print_result(input);
		} else {
			
		}
		
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
