package com.wjw.carSpeedMonitor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Test {

	//static Map<String, Integer> m = new HashMap<String, Integer>();
	public static void mian(String[] args) {
		//m.put("a",1);
//		final char value[] = {'a','b','c','\u0000'};
//		String str1 = "abc";
//		value[2] = 'e';
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
		String time = formatter.format(currentTime);
		System.out.println(time);
		
	}
}
