package com.wjw.carSpeedMonitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Date;
import java.util.Map;

public class WriterToFile implements IRichBolt{
	//static Date currentTime = new Date();
    ProducerTest producer = new ProducerTest();
    OutputCollector collector = null;
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


	public void print_result(Tuple input) {
		String result = input.getString(0)+"\t" + input.getInteger(1) + "\t" + input.getInteger(2) + "\n";
		producer.sendMsg(result,"gps-test");
		
    }
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		//print_result(input);

		collector.ack(input);
		
	}

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        producer.init();
    }
    @Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
    public static void main(String[] args) {
        //ProducerTest.sendMsg("wjw","gps-test");
    }
}
