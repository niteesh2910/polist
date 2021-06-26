package com.itradenetwork.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.KafkaMetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.LogbackMetricsAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.cloud.gcp.autoconfigure.storage.GcpStorageAutoConfiguration;

@SpringBootApplication(scanBasePackages = "com.itradenetwork", exclude = { KafkaAutoConfiguration.class,
		RedisAutoConfiguration.class, LogbackMetricsAutoConfiguration.class, KafkaMetricsAutoConfiguration.class,
		FlywayAutoConfiguration.class, GcpStorageAutoConfiguration.class })
public class OrderApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderApplication.class, args);
	}

}
