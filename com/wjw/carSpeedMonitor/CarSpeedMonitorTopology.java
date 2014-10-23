package com.wjw.carSpeedMonitor;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class CarSpeedMonitorTopology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new FileReaderSpout(),3);
		builder.setBolt("thresholdbolt", new ThresholdCalculatorBolt(),1).setNumTasks(5).shuffleGrouping("spout");
		builder.setBolt("statsbolt", new StatsBolt(),1).setNumTasks(5).fieldsGrouping("thresholdbolt", new Fields("carId","speed","city"));
		builder.setBolt("writerfilebolt",new WriterToFile(),1).fieldsGrouping("statsbolt", new Fields("carId","city","count"));
		
		Config conf = new Config();
		conf.setDebug(true);
		
		if(args != null && args.length > 0) {
			conf.setNumWorkers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			conf.setMaxTaskParallelism(3);
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("carSpeedMonitor", conf, builder.createTopology());
			Utils.sleep(60000);
			cluster.shutdown();
		}
	}

}
