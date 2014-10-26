package com.wjw.carSpeedMonitor;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;


public class FileReaderSpout extends BaseRichSpout {
	//static File file = new File("/root/origenalData-carSpeedMonitor-2014-05-06-18:58:09.txt");
    List<KafkaStream<byte[], byte[]>> streams = null;
	static private long timestamp = 0L;
	static InputStreamReader reader = null;
//	private static String carIds[] = new String[100];
//	static BufferedReader br = null;
//	static {
//		try {
//			reader = new InputStreamReader(new FileInputStream(file));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		br = new BufferedReader(reader);
//	}
	public SpoutOutputCollector _collector;
	long count = 0;
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
        final String topic =  "gps-data";
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

		
		int speed = Integer.valueOf(datums[6]);
		int city = 123;
		long time = Long.valueOf(datums[3]);
        int status = Integer.valueOf(datums[2]);
		Values value = new Values(carId,speed,city,time,status);
        if(time < 20121101000000L) {
            return null;
        }
		return value;

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
	

	@Override
	public void nextTuple() {

        String data = null;
        for(KafkaStream stream: streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while(it.hasNext()) {
                data = new String(it.next().message());
                break;
            }
        }
        Values v = getTuple(data);
        if(null != v) {
            _collector.emit(v,count);

            count++;
        }

       // ack(count);
		
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
		//carId,speed,city,time,status;
		declarer.declare(new Fields("carId","speed","city","time","status"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	public static void main(String[] args) {
		Values v = getTuple("w");
		for(int i = 0; i < 3; i++)
		System.out.println(v.get(i));
		
	}

}
