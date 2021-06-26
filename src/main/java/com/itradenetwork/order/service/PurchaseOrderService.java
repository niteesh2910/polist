package com.itradenetwork.order.service;

import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.springframework.core.env.Environment;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itradenetwork.auth.security.RequestContext;
import com.itradenetwork.auth.security.SecurityContextUserDetails;
import com.itradenetwork.framework.cache.CacheNameConstants;
import com.itradenetwork.framework.entity.CustomPageImpl;
import com.itradenetwork.framework.entity.FilterDataSet;
import com.itradenetwork.framework.entity.PurchaseOrder;
import com.itradenetwork.framework.utils.FilterAndInclusionSpecifications;
import com.itradenetwork.order.dao.PurchaseOrderDAO;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Slf4j
@Service
@AllArgsConstructor
public class PurchaseOrderService {

	private JedisPool jedisPool;
	private Environment environment;
	private ObjectMapper objectMapper;
	private RequestContext requestContext;
	private PurchaseOrderDAO purchaseOrderDAO;
	private SecurityContextUserDetails securityContextUserDetails;

	public Page<PurchaseOrder> getOrders(Pageable pageable, FilterAndInclusionSpecifications filterAndInclude,
			FilterDataSet filters) {
		log.info("PurchaseOrderGetService.getOrders starts with filters - {}", filters);
		Map<String, Object> params = new HashMap<>();
		Page<PurchaseOrder> orders = getOrdersFromRedisCache(pageable, filters);
		if (Objects.isNull(orders)) {
//			StringBuilder whereClause = new StringBuilder(generateWhereClause(params, filters.getScreenName(),
//					filters.getViewName(), isSearch, filterAndInclude));
//			OrderUtil.whereClauseCreator(whereClause, params, filters, securityContextUserDetails);
//			if (!CollectionUtils.isEmpty(filters.getStatusList())) {
//				whereClause.append(AND).append(securityContextUserDetails.isBroker() ? "CPS." : "P.")
//						.append("STATUS IN (:statusList) ");
//				params.put(STATUS_LIST, filters.getStatusList());
//			}
//			orders = purchaseOrderDAO.getPagedPurchaseOrders(pageable, whereClause.toString(), params,
//					filterAndInclude);
			if (Objects.nonNull(orders) && !CollectionUtils.isEmpty(orders.getContent())) {
				final Page<PurchaseOrder> redisOrders = orders;
				Executor reuestExecutor = requestContext.getRestTemplateExecutor();
				CompletableFuture.runAsync(() -> setPoListingToRedis(redisOrders, filters), reuestExecutor);
			}
		}
		log.info("PurchaseOrderGetService.getOrders exit for filters - {}", filters);
		return orders;
	}

	private void setPoListingToRedis(Page<PurchaseOrder> orders, FilterDataSet filters) {
		String date = "-14|14";
		String dateType = null;
//		UserPreference userPreference = userPreferenceRequestService.getUserPreferenceDetail(filters.getScreenName());
//		if (null != userPreference) {
//			date = userPreference.getPrefValue();
//			String key = userPreference.getPrefName();
//			if (key.contains("CreateDate")) {
//				dateType = ApplicationConstants.PO_SUBMISSION_DATE.substring(0, 1);
//			} else if (key.contains("ArrivalDate")) {
//				dateType = ApplicationConstants.ARRIVAL_DATE.substring(0, 1);
//			} else if (key.contains("ShipDate")) {
//				dateType = ApplicationConstants.SHIPPED_DATE.substring(0, 1);
//			} else if (key.contains("InvoiceDate")) {
//				dateType = ApplicationConstants.INVOICE_DATE.substring(0, 1);
//			}
//		}

		String keym = String.format("CID:%s:SN=%s:VN=%s:D=%s:DT=%s", securityContextUserDetails.getUserClientId(),
				filters.getScreenName().substring(0, 1), filters.getViewName().substring(0, 1), date, dateType);

		String pomapKey = "POMAP::" + keym;

		List<PurchaseOrder> pos = orders.getContent();
		Jedis jedis = jedisPool.getResource();
		try {
			String[] abc = new String[pos.size() * 2];
			String[] pokeys = new String[pos.size()];
			for (int i = 0; i < pos.size(); i++) {
				PurchaseOrder po = pos.get(i);
				String key = String.valueOf(
						getEnvSpecificNamespaces(CacheNameConstants.PURCHASE_ORDER_CACHE_NAME) + ":" + po.getId());
				abc[2 * i] = key;
				pokeys[i] = key;
				try {
					abc[2 * i + 1] = objectMapper.writeValueAsString(po);
				} catch (Exception e) {

				}
				jedis.sadd("PO-INV::" + po.getId(), pomapKey);
			}
			jedis.mset(abc);
			jedis.rpush(pomapKey, pokeys);
		} catch (Exception e) {
			//
		}
	}

	private Page<PurchaseOrder> getOrdersFromRedisCache(Pageable pageable, FilterDataSet filters) {
		String date = "-14|14";

		Date now = new Date();
		String dateType = null;
//		UserPreference userPreference = userPreferenceRequestService.getUserPreferenceDetail(filters.getScreenName());
//		if (null != userPreference) {
//			date = userPreference.getPrefValue();
//			String key = userPreference.getPrefName();
//			if (key.contains("CreateDate")) {
//				dateType = ApplicationConstants.PO_SUBMISSION_DATE.substring(0, 1);
//			} else if (key.contains("ArrivalDate")) {
//				dateType = ApplicationConstants.ARRIVAL_DATE.substring(0, 1);
//			} else if (key.contains("ShipDate")) {
//				dateType = ApplicationConstants.SHIPPED_DATE.substring(0, 1);
//			} else if (key.contains("InvoiceDate")) {
//				dateType = ApplicationConstants.INVOICE_DATE.substring(0, 1);
//			}
//		}

		String keym = String.format("CID:%s:SN=%s:VN=%s:D=%s:DT=%s", securityContextUserDetails.getUserClientId(),
				filters.getScreenName().substring(0, 1), filters.getViewName().substring(0, 1), date, dateType);
		Page<PurchaseOrder> orders = null;
		Jedis jedis = jedisPool.getResource();
		try {
//			List<String> keys = jedis.lrange("POMAP::" + keym, (pageable.getPageSize() * pageable.getPageNumber()),
//					(pageable.getPageSize() * (pageable.getPageNumber() + 1)));
			Map<String, String> cachedKeyValus = jedis.hgetAll(keym);
			long lastsyncedkey = Long.parseLong(cachedKeyValus.get("lastsyncTime"));
			Set<BigInteger> polist = objectMapper.readValue(cachedKeyValus.get("poids"),
					new TypeReference<Set<BigInteger>>() {
					});
			Set<String> afterLastSyncd = jedis.zrangeByScore(keym, lastsyncedkey, now.getTime());
			for (String poids : afterLastSyncd) {
				polist.addAll(objectMapper.readValue(poids, new TypeReference<Set<BigInteger>>() {
				}));
			}
			List<String> data = jedis.mget(polist.toArray(new String[0]));
			List<PurchaseOrder> pos = objectMapper.readValue(data.toString(), new TypeReference<List<PurchaseOrder>>() {
			});
			orders = new CustomPageImpl<>(pos);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return orders;
	}

	private String getEnvSpecificNamespaces(String cache) {
		return cache + "-" + environment.getProperty("ENV");
	}
}
