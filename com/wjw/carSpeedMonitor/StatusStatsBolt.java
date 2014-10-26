package com.wjw.carSpeedMonitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wjw on 14-10-26.
 */
public class StatusStatsBolt implements IRichBolt {
    OutputCollector collector = null;
    HashMap<Integer,Integer> hashMap = new HashMap<Integer, Integer>();
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        for(int i = 10000; i > 0; i--) {
            Math.atan(i);
        }
        String carId = input.getString(0);
        int time = input.getInteger(1);
        Integer count = hashMap.get(time);
        if(count == null ) {
            count = 1;
            hashMap.put(time,count);
        } else {
            count += 1;
            hashMap.put(time,count);
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
