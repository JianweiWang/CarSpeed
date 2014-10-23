package com.wjw.carSpeedMonitor;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ThresholdCalculatorBolt implements IBasicBolt{
	BasicOutputCollector _collector = null;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("carId","speed","city","time"));
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

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		if(!input.getString(0).equals("0000")) {
			System.out.println("threshold receive: " + input.toString());
			int speed = input.getInteger(1);
			if(speed >= 80) {
				collector.emit(new Values(input.getString(0),input.getInteger(1),input.getInteger(2),input.getLong(3)));
			}
		} else {
			collector.emit(new Values(input.getString(0),input.getInteger(1),input.getInteger(2),input.getLong(3)));
		}
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	

	

}
