package com.wjw.carSpeedMonitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by wjw on 14-10-26.
 */
public class StatusFilterBolt implements IRichBolt {
    OutputCollector collector = null;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        for(int i = 3000; i > 0; i --) {
            Math.atan(i);
        }
        String carId = input.getString(0);
        int status = input.getInteger(4);
        long time = input.getLong(3);
        int segment = -1;
        if(1 == status ) {
            int timeSegmeng = Integer.valueOf(String.valueOf(time).substring(8,10));
            if(timeSegmeng >= 0 && timeSegmeng < 4) {
                segment = 0;
            } else if (timeSegmeng >= 4 && timeSegmeng < 8) {
                segment = 1;
            }  else if (timeSegmeng >= 8 && timeSegmeng < 12) {
                segment = 2;
            } else if (timeSegmeng >= 12 && timeSegmeng < 16) {
                segment = 3;
            } else if (timeSegmeng >= 16 && timeSegmeng < 20) {
                segment = 4;
            } else if (timeSegmeng >= 20 && timeSegmeng < 24) {
                segment = 5;
            }
            collector.emit(new Values(carId,segment));
            collector.ack(input);
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("carId","time"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
