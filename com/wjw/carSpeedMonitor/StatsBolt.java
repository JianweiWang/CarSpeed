package com.wjw.carSpeedMonitor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StatsBolt implements IBasicBolt{
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
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		if(!input.getString(0).equals("0000")) {
			System.out.println("----------------------------statsbolt receive: " + input.toString());
//			List<Object> values = input.getValues(); 
//			int time = (int) values.get(3);
			long time = input.getLong(3);
			
			String carId = input.getString(0);
			
			System.out.println("----------------------------statsbolt receive carId " + carId);
			
			startTime = carIdToTime.get(carId);
			
			//Values v = new Values(carId,input.getInteger(2));
			if(startTime == null) {
				System.out.println("--------------------------statsbolt: carIdToTime.get(carId): "+ carIdToTime.get(carId)+"-----------------");
				System.out.println("--------------------------statsbolt: carIdToTime.get(carId) == null-----------------");
				carIdToTime.put(carId, time);
				System.out.println("--------------------------statsbolt: carIdToTime.get(carId): "+ carIdToTime.get(carId)+"-----------------");
				counts.put(carId, 1);
			//	startTime = time;
				
			} else {
				System.out.println("--------------------------statsbolt: carIdToTime.get(carId) != null-----------------");
				//int starTime = carIdToTime.get(carId);
				System.out.println("====================== carIdToTime.get(carId)  "+ carIdToTime.get(carId) +"============= counts.get(v) "+ counts.get(carId)
					+"=============time " + time);
				if(time - startTime <= 5){
					int count = counts.get(carId);
					System.out.println("------------------------statsbolt: time - startTime <= 5,count = " + count+"----------------");

					count++;
					counts.put(carId, count);
				} else {
					//startTime = time;
					System.out.println("--------------------------statsbolt: time - startTime > 5----------------");
					System.out.println("--------------------------statsbolt: carIdToTime.get(carId): "+ carIdToTime.get(carId)+"-----------------");
					System.out.println("--------------------------statsbolt: time: "+ time+"-----------------");
					if(counts.get(carId) != null && counts.get(carId) >= 2) {
						System.out.println("statsbolt: counts.get(v) >= 2");
						collector.emit(new Values(input.getString(0),input.getInteger(2),counts.get(carId)));
						carIdToTime.put(carId, time);
						
					} else if (counts.get(carId) == null ) {
						System.out.println("============================counts.get(v) = null!");
					} 
				}
			}
			
		} else {
			Set<String> list1 = counts.keySet();
			Iterator iter = list1.iterator();
			while(iter.hasNext())
			{
				String v = (String) iter.next();
				System.out.println("statbolt============: "+v.toString());
				if(counts.get(v) > 1) {
					System.out.println("statbolt============: "+v.toString()+"\t"+counts.get(v));
					collector.emit(new Values(v,input.getInteger(2),counts.get(v)));
				}
			}
			System.out.println("----------------------------statsbolt receive: " + input.toString());
			collector.emit(new Values(input.getString(0),input.getInteger(2),-1));
		}
		
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
