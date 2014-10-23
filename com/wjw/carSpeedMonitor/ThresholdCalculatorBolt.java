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

public class ThresholdCalculatorBolt implements IRichBolt{
	OutputCollector collector = null;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("carId","speed","city","time","status"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
        for(int i = 3000; i > 0; i--) {
            Math.atan(i);
        }
			//System.out.println("threshold receive: " + input.toString());
			int speed = input.getInteger(1);
			if(speed >= 60) {
				collector.emit(new Values(input.getString(0),input.getInteger(1),input.getInteger(2),input.getLong(3),input.getInteger(4)));
			}
        collector.ack(input);

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	

	

}
