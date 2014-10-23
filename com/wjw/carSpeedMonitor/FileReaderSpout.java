package com.wjw.carSpeedMonitor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class FileReaderSpout implements IRichSpout{
	//static File file = new File("/root/origenalData-carSpeedMonitor-2014-05-06-18:58:09.txt");
    List<KafkaStream<byte[], byte[]>> streams = null;
	static private long timestamp = 0L;
	static InputStreamReader reader = null;
	private static String carIds[] = new String[100];
	static BufferedReader br = null;
	static {
		try {
			reader = new InputStreamReader(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		br = new BufferedReader(reader);
	}
	public SpoutOutputCollector _collector;
	int count = 0;
	static Date currentTime = new Date();

    private static ConsumerConfig createConsumerConfig(String a_zookeeper,
                                                       String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "1000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }
	//BufferedReader fileReader = null;
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
        final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(createConsumerConfig("192.168.0.8:2181", "group-1"));
        final String topic =  "page_visits";
        //ExecutorService executor;
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        streams = consumerMap.get(topic);
//		String file = "/root/logfile.txt";
//		try {
//			 fileReader = new BufferedReader(new FileReader(new File(file)));
//		} catch (Exception e) {
//			e.printStackTrace();
//			System.exit(1);
//			
//		}
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	private static int getRandomTime(int start) {
		try {
			//SimpleDateFormat format = new SimpleDateFormat("hh:ss");
			
			Random rand = new Random(System.currentTimeMillis());
			return start + rand.nextInt(60);
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}
		return 0;
		
	}
	private static Values getTuple(String data) {

        String[] datums = data.split(",");
        String carId = datums[0];
		char alphabet[] = new char[26];


		for(int i = 0; i < 26; i++)
		{
			alphabet[i] = (char) ('a' + i);
		}

		Random rd = new Random(System.currentTimeMillis());
		if(carIds[0] == null)
		for(int i = 0 ;i < 100;i++) {
			carIds[i] = (String)(String.valueOf(alphabet[rd.nextInt(26)]) + String.valueOf(alphabet[rd.nextInt(26)]) + String.valueOf(rd.nextInt(1000)));
		}

		//System.out.println(carId);
		
		int speed = (rd.nextInt(120)+50)%120;
		int city = (rd.nextInt(30)+20)%20;
		long time = timestamp++;
		Values value = new Values(carId,speed,city,time);
		return value;
//		if(file.isFile() && file.exists()) {
//			try {
//				
//				String line = null; 
//				if((line = br.readLine()) != null) {
//					String str[] = line.split("\t");
//					Values value = new Values(str[0],Integer.valueOf(str[1]),Integer.valueOf(str[2]),Integer.valueOf(str[2]));
//					return value;
//				}
//				//return;
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		return null;
		
		
	}
	public void print_result(Values input) {
		
		FileWriter fw = null;
		//Date currentTime = new Date();
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
			String time = formatter.format(currentTime);

			//System.out.println("prepare to print the statistics");
			try {
				fw = new FileWriter(new File("/root/origenalData-carSpeedMonitor-"+time+".txt"),true);
				fw.write(input.get(0)+"\t" +  input.get(1).toString() + "\t" + input.get(2).toString() + "\t"+ input.get(3).toString() + "\n");
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
	
//	public String getLine() {
//		
//		if(file.isFile() && file.exists()) {
//			try {
//				reader = new InputStreamReader(new FileInputStream(file));
//				br = new BufferedReader(reader);
//				String line = null; 
//				while((line = br.readLine()) != null) {
//					String str[] = line.split("\t");
//					_collector.emit(new Values(str[0],Integer.valueOf(str[1]),Integer.valueOf(str[2]),Integer.valueOf(str[2])));
//				}
//				System.out.println("emitting \"0000\"");
//				_collector.emit(new Values("0000",0,0,0));
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		return null;
//	}
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		//long start = System.currentTimeMillis();
//		List<Values> valueList = new ArrayList<Values>(10);
//		valueList.add(new Values("ab123",12,2,50));
//		valueList.add(new Values("ab121",92,2,58));
//		valueList.add(new Values("ab121",100,2,60));
//		valueList.add(new Values("ab123",30,2,53));
//		valueList.add(new Values("ab123",12,2,50));
//		valueList.add(new Values("ab123",90,2,67));
//		valueList.add(new Values("ab123",95,2,67));
//		valueList.add(new Values("ab123",95,2,67));
//		valueList.add(new Values("ab123",12,2,89));
//		valueList.add(new Values("ab123",49,2,36));
		//File file = new File("/root/origenalData-carSpeedMonitor-2014-05-06-18:58:09.txt");
//		if(file.isFile() && file.exists()) {
//			try {
//				
//				String line = null; 
//				if((line = br.readLine()) != null) {
//					String str[] = line.split("\t");
//					_collector.emit(new Values(str[0],Integer.valueOf(str[1]),Integer.valueOf(str[2]),Integer.valueOf(str[2])));
//				} else {
//					System.out.println("emitting \"0000\"");
//					_collector.emit(new Values("0000",0,0,0));
//					return;
//				}
//				
//				//return;
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		while(count < 6000*24) {
//		//	_collector.emit(valueList.get(count));
//			Values v = getTuple();
//			//Tuple t = (Tuple) v;
//			print_result(v);
//			_collector.emit(v);
//			count++;
//		}
//		if(count == 6000*24) {
//		//	Utils.sleep(1000*60);
//			System.out.println("emitting \"0000\"");
//			_collector.emit(new Values("0000",0,0,0L));
//			count++;
//		} else {
//			return;
//		}
        String data = null;
        for(KafkaStream stream: streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while(it.hasNext()) {
                data = new String(it.next().message());
                // System.out.println("=============================================" + sentence);
                break;
            }
        }
        Values v = getTuple(data);
        _collector.emit(v);
        count++;

		
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		//ack(msgId);
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//字段：车辆ID、触发事件、运营状态、GPS时间、GPS经度、GPS纬度、GPS速度、GPS方位、GPS状态。
		declarer.declare(new Fields("carId","speed","city","time"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	public static void main(String[] args) {
		Values v = getTuple();
		for(int i = 0; i < 3; i++)
		System.out.println(v.get(i));
		
	}

}
