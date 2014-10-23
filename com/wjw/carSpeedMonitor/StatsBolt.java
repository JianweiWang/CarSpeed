package com.wjw.carSpeedMonitor;

import java.text.SimpleDateFormat;
import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StatsBolt implements IRichBolt{
    OutputCollector collector = null;
	Long startTime = null;
	private static Map <String,Integer> counts = new HashMap<String,Integer>();//保存5分钟内超速2次以上的车辆信息；
	private static Map <String,Long> carIdToTime = new HashMap<String,Long>();//保存每辆车最近的开始计时时间；
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("carId","city","count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub


        for(int i = 30000; i > 0; i--) {
            Math.atan(i);
        }
			long time = input.getLong(3);
            String time_seconds = String.valueOf(time - 20121100000000L);
            long seconds = (Long.valueOf(time_seconds.substring(0,1)) - 1) * 24 * 3600 +
                    Long.valueOf(time_seconds.substring(1,3)) * 3600
                    + Long.valueOf(time_seconds.substring(3,5)) * 60 + Long.valueOf(time_seconds.substring(5,7));


			
			String carId = input.getString(0);
			

			
			startTime = carIdToTime.get(carId);

			
			//Values v = new Values(carId,input.getInteger(2));
			if(startTime == null) {

				carIdToTime.put(carId, time);
				counts.put(carId, 1);
			} else {
				if(time - startTime <= 4 * 3600){
					int count = counts.get(carId);

					count++;
					counts.put(carId, count);
				} else {
					if(counts.get(carId) != null && counts.get(carId) >= 2) {
						//System.out.println("statsbolt: counts.get(v) >= 2");
						//collector.emit(new Values(input.getString(0),input.getInteger(2),counts.get(carId)));
						carIdToTime.put(carId, time);
						
					} else if (counts.get(carId) == null ) {
						//System.out.println("============================counts.get(v) = null!");
					} 
				}
			}
			

			Set<String> list1 = counts.keySet();
			Iterator iter = list1.iterator();
//			while(iter.hasNext())
//			{
//				String v = (String) iter.next();
//				//System.out.println("statbolt============: "+v.toString());
//				if(counts.get(v) > 1) {
//					//System.out.println("statbolt============: "+v.toString()+"\t"+counts.get(v));
//					collector.emit(new Values(v,input.getInteger(2),counts.get(v)));
//				}
//			}
			//System.out.println("----------------------------statsbolt receive: " + input.toString());
			//collector.emit(new Values(input.getString(0),input.getInteger(2),-1));
        collector.ack(input);

		}


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
