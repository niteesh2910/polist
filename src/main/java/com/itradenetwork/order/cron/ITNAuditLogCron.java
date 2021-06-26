package com.itradenetwork.order.cron;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.itradenetwork.framework.cache.CacheNameConstants;
import com.itradenetwork.framework.entity.PurchaseOrder;
import com.itradenetwork.order.dao.PurchaseOrderDAO;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

@Slf4j
@Component
@EnableScheduling
public class ITNAuditLogCron {

	private boolean isCronEnabled = false;

	@Autowired
	private JedisPool jedisPool;
	@Autowired
	private Environment environment;
	@Autowired
	private ObjectMapper objectMapper;
	@Autowired
	private PurchaseOrderDAO purchaseOrderDAO;

	@PostConstruct
	public void init() {
		String cronEnabled = environment.getProperty("cron.enabled");
		isCronEnabled = Boolean.parseBoolean(cronEnabled);
		log.info("ITNAuditLogCron.init cronEnabled - {}", cronEnabled);
	}

//	@Scheduled(fixedRate = 10000)
	public void invokeITNLogs() {
		Date now = new Date();
		double clientPOListKey = now.getTime();
//		if (isCronEnabled) {
			List<BigInteger> poIds = purchaseOrderDAO.getItnLogPurchaseOrders();
			if (!CollectionUtils.isEmpty(poIds)) {
				Jedis jedis = jedisPool.getResource();
				List<PurchaseOrder> pos = purchaseOrderDAO.getPurchaseOrders(poIds);
				String[] abc = new String[pos.size() * 2];
				Transaction transaction = jedis.multi();
				Map<BigInteger, List<BigInteger>> clientPOMap = new HashMap<>();
				for (int i = 0; i < pos.size(); i++) {
					PurchaseOrder po = pos.get(i);
					String key = String.valueOf(
							getEnvSpecificNamespaces(CacheNameConstants.PURCHASE_ORDER_CACHE_NAME) + ":" + po.getId());
					abc[2 * i] = key;
					try {
						abc[2 * i + 1] = objectMapper.writeValueAsString(po);
					} catch (Exception e) {
						log.error("ITNAuditLogCron.invokeITNLogs error occured while po json converison - {}",
								e.getMessage(), e);
					}
					clientPOMap.computeIfAbsent(po.getClientId(), k -> new ArrayList<>());
					clientPOMap.computeIfAbsent(po.getVendorId(), k -> new ArrayList<>());
					clientPOMap.get(po.getClientId()).add(po.getId());
					clientPOMap.get(po.getVendorId()).add(po.getId());
				}

				for (Map.Entry<BigInteger, List<BigInteger>> e : clientPOMap.entrySet()) {
					transaction.zadd(getEnvSpecificNamespaces("CLIENT_PO_LOG_LIST") + ":" + e.getKey(), clientPOListKey,
							String.valueOf(e.getValue()));
				}
				transaction.mset(abc);
				transaction.exec();
			}
//		}
	}

	private String getEnvSpecificNamespaces(String cache) {
		return cache + "-" + environment.getProperty("ENV");
	}
}
