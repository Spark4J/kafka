package com.example.partitioner;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class Partitioner01 implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// 控制分区
		/*
		可以添加判断 对某一类数据专门存放到指定分区
		 */
		return 1;
	}

	@Override
	public void close() {

	}

}
