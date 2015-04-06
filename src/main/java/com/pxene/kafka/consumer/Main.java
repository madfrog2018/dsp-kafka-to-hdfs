package com.pxene.kafka.consumer;

/**
 * 
 * read log from kafka to hdfs
 * @author yanghu
 * 2015年3月26日
 *
 */
public class Main {

	public static void main(String[] args) {
		String zk = "pxene01:4181,pxene03:4181,pxene04:4181";
		String groupid = "pxene_test";
		String motionTopic = "motionlogs";
		String motionRoot = "hdfs://pxene01:9000/user/root/motion_test";
		int threads = 5; //the same with the num of kafka partition
		

		
		//展现、曝光
		HDFSWriter motionWriter = new HDFSWriter();
		motionWriter.init(motionTopic, motionRoot, zk, groupid, threads);
    }

}
