package com.itradenetwork.order.service;

import static com.itradenetwork.framework.utils.ApplicationConstants.PROPERTY_HOST_URL;
import static com.itradenetwork.framework.utils.ApplicationConstants.AppUri.BASE_URI_CONSTANT;
import static com.itradenetwork.framework.utils.ApplicationConstants.AppUri.USER_APP_URI;
import static com.itradenetwork.framework.utils.ApplicationConstants.UserUrl.URL_FOR_USER_PREFERENCE;
import static com.itradenetwork.framework.utils.SQLConstants.AND;
import static com.itradenetwork.framework.utils.SQLConstants.BETWEEN_START_DATE_AND_END_DATE;
import static com.itradenetwork.framework.utils.SQLConstants.CLIENT_ID;
import static com.itradenetwork.framework.utils.SQLConstants.LANGUAGE_CODE;
import static com.itradenetwork.framework.utils.SQLConstants.VENDOR_ID;
import static com.itradenetwork.framework.utils.SQLConstants.WHERE;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itradenetwork.auth.security.RequestContext;
import com.itradenetwork.auth.security.SecurityContextUserDetails;
import com.itradenetwork.auth.service.ApiProxy;
import com.itradenetwork.framework.cache.CacheNameConstants;
import com.itradenetwork.framework.dto.CatalogRestrictions;
import com.itradenetwork.framework.dto.CategoryRestrictions;
import com.itradenetwork.framework.dto.CompanyRestrictions;
import com.itradenetwork.framework.dto.LocationRestrictions;
import com.itradenetwork.framework.dto.Member;
import com.itradenetwork.framework.dto.UserRestrictions;
import com.itradenetwork.framework.dto.UserRestrictions.UserRestrictionsBuilder;
import com.itradenetwork.framework.entity.CustomPageImpl;
import com.itradenetwork.framework.entity.FilterDataSet;
import com.itradenetwork.framework.entity.Product;
import com.itradenetwork.framework.entity.PurchaseOrder;
import com.itradenetwork.framework.entity.PurchaseOrderItem;
import com.itradenetwork.framework.entity.UserPreference;
import com.itradenetwork.framework.utils.ApplicationConstants;
import com.itradenetwork.framework.utils.ApplicationConstants.APIHeaderKeys;
import com.itradenetwork.framework.utils.FilterAndInclusionSpecifications;
import com.itradenetwork.framework.utils.GeneralUtils;
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
	private ApiProxy<UserPreference, UserPreference> userPrefProxy;

	public Page<PurchaseOrder> getOrders(Pageable pageable, FilterAndInclusionSpecifications filterAndInclude,
			FilterDataSet filters) {
		log.info("PurchaseOrderService.getOrders starts with filters - {}", filters);
		Calendar endCal = Calendar.getInstance();
		Calendar startCal = Calendar.getInstance();
		UserPreference userPreference = getUserPref(startCal, endCal, filters.getScreenName());
		String redisKey = generateUserKey(filters, userPreference, pageable);
		Page<PurchaseOrder> orders = getOrdersFromRedisCache(filters, userPreference, pageable, redisKey,
				startCal.getTime(), endCal.getTime());
		if (Objects.isNull(orders)) {
			Map<String, Object> params = new HashMap<>();
			boolean isSearch = !CollectionUtils.isEmpty(filterAndInclude.getSerachCriteriaList())
					|| !CollectionUtils.isEmpty(filterAndInclude.getSerachCriteriaListForORCondition());
			StringBuilder whereClause = new StringBuilder(generateWhereClause(params, filters.getScreenName(), isSearch,
					filterAndInclude, userPreference, startCal, endCal, filters));
			orders = purchaseOrderDAO.getPagedPurchaseOrders(pageable, whereClause.toString(), params,
					filterAndInclude);
			if (Objects.nonNull(orders) && !CollectionUtils.isEmpty(orders.getContent())) {
				final Page<PurchaseOrder> redisOrders = orders;
				Executor reuestExecutor = requestContext.getRestTemplateExecutor();
				CompletableFuture.runAsync(() -> setPoListingToRedis(redisOrders, userPreference, pageable, redisKey),
						reuestExecutor);
			}
		}
		log.info("PurchaseOrderService.getOrders exit for filters - {}", filters);
		return orders;
	}

	private String generateWhereClause(Map<String, Object> params, String screenName, boolean isSearch,
			FilterAndInclusionSpecifications filterAndInclude, UserPreference userPreference, Calendar startCal,
			Calendar endCal, FilterDataSet filters) {
		log.info("PurchaseOrderService.generateWhereClause screenName - {}", screenName);
		StringBuilder whereClause = new StringBuilder(WHERE);

		if (!isSearch) {
			log.info("PurchaseOrderService.generateWhereClause filteringColumn - {}",
					userPreference.getFilteringColumn());
			params.put("startDate", startCal.getTime());
			params.put("endDate", endCal.getTime());
			setDateFiltersToClause(whereClause, userPreference.getFilteringColumn());
		}

		params.put(LANGUAGE_CODE, LocaleContextHolder.getLocale().getLanguage());
		setClientsFiltersInWhereClause(whereClause, params, isSearch);
		setFiltersToWhereClasue(whereClause, params, filterAndInclude, screenName, isSearch, filters);
		log.info("PurchaseOrderService.generateWhereClause whereClause - {} params - {}", whereClause, params);
		return whereClause.toString();
	}

	private StringBuilder setFiltersToWhereClasue(StringBuilder whereClause, Map<String, Object> params,
			FilterAndInclusionSpecifications filterAndInclude, String screenName, boolean isSearch,
			FilterDataSet filters) {
		if (!CollectionUtils.isEmpty(filters.getShipToIds())) {
			whereClause.append(" AND P.DELIVEREDLOCID IN (:shipToIds) ");
			params.put("shipToIds", filters.getShipToIds());
		}
		if (!CollectionUtils.isEmpty(filters.getShipFromIds())) {
			whereClause.append(" AND P.PICKUPLOCATIONID IN (:shipFromIds) ");
			params.put("shipFromIds", filters.getShipFromIds());
		}
		if (!CollectionUtils.isEmpty(filters.getVendorIds())) {
			whereClause.append(" AND P.SELLERMEMBERCOMPANYID IN (:sellers) ");
			params.put("sellers", filters.getVendorIds());
		}
		if (!CollectionUtils.isEmpty(filters.getCustomerIds())) {
			whereClause.append(" AND P.BUYERMEMBERCOMPANYID IN (:sellers) ");
			params.put("sellers", filters.getCustomerIds());
		}
		if (!CollectionUtils.isEmpty(filters.getCarriers())) {
//			whereClause.append(" AND P.BUYERMEMBERCOMPANYID IN (:sellers) ");
//			params.put("sellers", filters.getCustomerIds());
		}
		return whereClause;
	}

	public UserPreference getUserPref(Calendar startCal, Calendar endCal, String screenName) {
		int noOfBackDays = -14;
		int noOfForwardDays = 14;
		String filteringColumn = null;
		UserPreference userPreference = null;
		if (StringUtils.hasLength(screenName)) {
			userPreference = getUserPreferenceDetail(screenName);
			try {
				if (null != userPreference) {
					String[] userPref = userPreference.getPrefValue().split("\\|");
					String key = userPreference.getPrefName();
					noOfBackDays = Integer.valueOf(userPref[0]);
					noOfForwardDays = Integer.valueOf(userPref[1]);
					if (key.contains("CreateDate")) {
						filteringColumn = ApplicationConstants.PO_SUBMISSION_DATE;
					} else if (key.contains("ArrivalDate")) {
						filteringColumn = ApplicationConstants.ARRIVAL_DATE;
					} else if (key.contains("ShipDate")) {
						filteringColumn = ApplicationConstants.SHIPPED_DATE;
					} else if (key.contains("InvoiceDate")) {
						filteringColumn = ApplicationConstants.INVOICE_DATE;
					}
				}
			} catch (Exception e) {
				log.info("Exception occured while fething the day details");
			}
		}
		if (null == userPreference) {
			userPreference = new UserPreference();
			userPreference.setPrefValue("-14|14|true");
		}
		// In case there is no preference set
		if (!StringUtils.hasLength(filteringColumn)) {
			filteringColumn = securityContextUserDetails.isBuyer() ? ApplicationConstants.ARRIVAL_DATE
					: ApplicationConstants.SHIPPED_DATE;
			userPreference.setPrefName(securityContextUserDetails.isBuyer() ? "ArrivalDate" : "ShipDate");
		}

		startCal.add(Calendar.DATE, noOfBackDays);
		GeneralUtils.setStartTimeForCalendar(startCal);
		endCal.add(Calendar.DATE, noOfForwardDays);
		GeneralUtils.setEndTimeForCalendar(endCal);
		userPreference.setFilteringColumn(filteringColumn);
		return userPreference;
	}

	public UserPreference getUserPreferenceDetail(String screenName) {
		log.info("PurchaseOrderService.getUserPreferenceDetail screenName:: {}", screenName);
		UserPreference userPref = null;
		String hostUrl = environment.getRequiredProperty(PROPERTY_HOST_URL);
		String userUrl = hostUrl + USER_APP_URI + BASE_URI_CONSTANT;
		String url = new StringBuilder(userUrl).append(URL_FOR_USER_PREFERENCE).append("?screenName=")
				.append(screenName).toString();
		try {
			userPref = userPrefProxy.invoke(headers(), url, HttpMethod.GET,
					new ParameterizedTypeReference<UserPreference>() {
					});
		} catch (HttpClientErrorException | HttpServerErrorException ex) {
			if (Integer.valueOf(404).equals(ex.getStatusCode().value())
					|| Integer.valueOf(503).equals(ex.getStatusCode().value())
					|| Integer.valueOf(502).equals(ex.getStatusCode().value())) {
				log.error(
						"PurchaseOrderService.getUserPreferenceDetail Error occured - Service Unavailable or Not Found: "
								+ url,
						ex);
			}
		} catch (Exception e) {
			log.info("PurchaseOrderService.getUserPreferenceDetail Error occured while executing", e);
		}
		return userPref;
	}

	private HttpHeaders headers() {
		HttpHeaders headers = new HttpHeaders();

		headers.set(APIHeaderKeys.X_AUTHORIZATION, "Bearer " + securityContextUserDetails.getSecurityToken());
		headers.set(APIHeaderKeys.REALM, securityContextUserDetails.getRealmName());
		headers.set(APIHeaderKeys.OB_CLIENT_ID, String.valueOf(securityContextUserDetails.getOrgId()));
		headers.set(APIHeaderKeys.PREFERRED_USERNAME, securityContextUserDetails.getUserContext().getUsername());
		headers.set(APIHeaderKeys.TOKEN, securityContextUserDetails.getSecurityToken());
		headers.set(APIHeaderKeys.CTYPE, securityContextUserDetails.getUserContext().getCType());
		headers.set(APIHeaderKeys.NAME, securityContextUserDetails.getUserContext().getName());
		headers.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

		return headers;
	}

	private void setDateFiltersToClause(StringBuilder whereClause, String filteringColumn) {
		log.debug("PurchaseOrderService.setDateFiltersToClause starts for filteringColumn - {}", filteringColumn);
		if (securityContextUserDetails.isBuyer()) {
			whereClause.append(" P.DATEREQUIRED ").append(BETWEEN_START_DATE_AND_END_DATE);
		} else {
			whereClause.append(" P.DATESHIPPING ").append(BETWEEN_START_DATE_AND_END_DATE);
		}
		log.debug("PurchaseOrderService.setDateFiltersToClause exit for filteringColumn - {}", filteringColumn);
	}

	private void setClientsFiltersInWhereClause(StringBuilder whereClause, Map<String, Object> params,
			boolean isSearch) {
		if (!isSearch) {
			whereClause.append(AND);
		}
		if (securityContextUserDetails.isVendor()) {
			whereClause.append(" P.SELLERMEMBERCOMPANYID = :vendorId ");
			params.put(VENDOR_ID, securityContextUserDetails.getOrgId());
//			params.put(EXEMPT_STATUS_LIST,
//					Stream.of(CommonEnums.POStatus.DRAFT.getStatusId(), CommonEnums.POStatus.DRAFTDELETED.getStatusId(),
//							CommonEnums.POStatus.DELETED.getStatusId()).collect(Collectors.toSet()));
		} else if (securityContextUserDetails.isBuyer()) {
			whereClause.append(" P.BUYERMEMBERCOMPANYID = :clientId ");
			params.put(CLIENT_ID, securityContextUserDetails.getOrgId());
//			params.put(EXEMPT_STATUS_LIST, Stream
//					.of(CommonEnums.POStatus.DRAFTDELETED.getStatusId(), CommonEnums.POStatus.DELETED.getStatusId())
//					.collect(Collectors.toSet()));
		} else {
			whereClause.append(" (P.BUYERMEMBERCOMPANYID = :clientId OR P.SELLERMEMBERCOMPANYID =:clientId) ");
			params.put(CLIENT_ID, securityContextUserDetails.getOrgId());
//			params.put(EXEMPT_STATUS_LIST, Stream
//					.of(CommonEnums.POStatus.DRAFTDELETED.getStatusId(), CommonEnums.POStatus.DELETED.getStatusId())
//					.collect(Collectors.toSet()));
		}
//		whereClause.append(" AND P.STATUS NOT IN (:exemptStatusList) ");
	}

	private void setPoListingToRedis(Page<PurchaseOrder> orders, UserPreference userPreference, Pageable pageable,
			String keym) {
		Date now = new Date();

		Map<String, String> lastSyncedMap = new HashMap<>();
		lastSyncedMap.put("lastsyncTime", String.valueOf(now.getTime()));
		Set<BigInteger> poids = new HashSet<>();
		List<PurchaseOrder> pos = orders.getContent();
		Jedis jedis = jedisPool.getResource();

		List<BigInteger> orderids = orders.stream().map(PurchaseOrder::getId).collect(Collectors.toList());
		Map<BigInteger, List<PurchaseOrderItem>> items = purchaseOrderDAO.getPurchaseOrderItems(orderids);

		try {
			String[] abc = new String[pos.size() * 2];
			String[] pokeys = new String[pos.size()];
			for (int i = 0; i < pos.size(); i++) {
				PurchaseOrder po = pos.get(i);
				po.setPurchaseOrderItems(items.get(po.getId()));
				poids.add(po.getId());
				String key = String.valueOf(
						getEnvSpecificNamespaces(CacheNameConstants.PURCHASE_ORDER_CACHE_NAME) + ":" + po.getId());
				abc[2 * i] = key;
				pokeys[i] = key;
				try {
					abc[2 * i + 1] = objectMapper.writeValueAsString(po);
				} catch (Exception e) {

				}
			}
			lastSyncedMap.put("poids", objectMapper.writeValueAsString(poids));
			jedis.mset(abc);
			jedis.hmset(keym, lastSyncedMap);
		} catch (Exception e) {
			//
		}
	}

	private Page<PurchaseOrder> getOrdersFromRedisCache(FilterDataSet filters, UserPreference userPreference,
			Pageable pageable, String keym, Date startDate, Date endDate) {
		Date now = new Date();

		String dateType = null;
		if (null != userPreference) {
			String key = userPreference.getPrefName();
			if (key.contains("CreateDate")) {
				dateType = ApplicationConstants.PO_SUBMISSION_DATE;
			} else if (key.contains("ArrivalDate")) {
				dateType = ApplicationConstants.ARRIVAL_DATE;
			} else if (key.contains("ShipDate")) {
				dateType = ApplicationConstants.SHIPPED_DATE;
			} else if (key.contains("InvoiceDate")) {
				dateType = ApplicationConstants.INVOICE_DATE;
			}
		}

		Page<PurchaseOrder> orders = null;
		Jedis jedis = jedisPool.getResource();
		try {
			Map<String, String> cachedKeyValues = jedis.hgetAll(keym);
			if (!CollectionUtils.isEmpty(cachedKeyValues) && StringUtils.hasLength(cachedKeyValues.get("poids"))) {
				long lastsyncedkey = Long.parseLong(cachedKeyValues.get("lastsyncTime"));
				Set<String> polist = objectMapper.readValue(cachedKeyValues.get("poids"),
						new TypeReference<Set<String>>() {
						});
				Set<String> afterLastSyncd = jedis.zrangeByScore(
						getEnvSpecificNamespaces("CLIENT_PO_LOG_LIST") + ":" + securityContextUserDetails.getOrgId(),
						lastsyncedkey, now.getTime());
				for (String poids : afterLastSyncd) {
					polist.addAll(objectMapper.readValue(poids, new TypeReference<Set<String>>() {
					}));
				}
				List<String> poidkeys = polist.stream()
						.map(c -> getEnvSpecificNamespaces(CacheNameConstants.PURCHASE_ORDER_CACHE_NAME) + ":" + c)
						.collect(Collectors.toList());
				List<String> data = jedis.mget(poidkeys.toArray(new String[0]));
				List<PurchaseOrder> pos = objectMapper.readValue(data.toString(),
						new TypeReference<List<PurchaseOrder>>() {
						});

				pos = filterPurchaseOrders(filters, dateType, pageable, pos, startDate, endDate);
				Set<BigInteger> latestpos = pos.stream().map(PurchaseOrder::getId).collect(Collectors.toSet());
				Map<String, String> lastSyncedMap = new HashMap<>();
				lastSyncedMap.put("lastsyncTime", String.valueOf(now.getTime()));
				lastSyncedMap.put("poids", objectMapper.writeValueAsString(latestpos));
				jedis.hmset(keym, lastSyncedMap);
				orders = new CustomPageImpl<>(pos);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return orders;
	}

	private String getEnvSpecificNamespaces(String cache) {
		return cache + "-" + environment.getProperty("ENV");
	}

	private List<PurchaseOrder> filterPurchaseOrders(FilterDataSet filters, String dateType, Pageable pageable,
			List<PurchaseOrder> pos, Date startDate, Date endDate) {

		List<PurchaseOrder> purchaseOrders = new ArrayList<>();

		Jedis jedis = jedisPool.getResource();
		Member member = null;
		UserRestrictions restrictions = null;
		Map<String, String> userMap = jedis
				.hgetAll(getEnvSpecificNamespaces("USER") + ":" + securityContextUserDetails.getUserId());
		try {
			member = objectMapper.readValue(userMap.get("userInfo"), new TypeReference<Member>() {
			});
			restrictions = objectMapper.readValue(userMap.get("restrictions:" + member.getCurrentPortal()),
					new TypeReference<UserRestrictions>() {
					});

			for (int i = 0; i < pos.size(); i++) {

				boolean isValid = true;
				PurchaseOrder purchaseOrder = pos.get(i);

				List<BigInteger> productIds = purchaseOrder.getPurchaseOrderItems().stream()
						.filter(item -> Objects.nonNull(item.getProductId())).map(PurchaseOrderItem::getProductId)
						.collect(Collectors.toList());

				List<String> data = jedis.mget(productIds.toArray(new String[0]));
				List<Product> products = objectMapper.readValue(data.toString(), new TypeReference<List<Product>>() {
				});
				
				if (dateType.equals(ApplicationConstants.ARRIVAL_DATE)
						&& (Objects.isNull(purchaseOrder.getParsedArrivalDate())
								|| purchaseOrder.getParsedArrivalDate().after(endDate)
								|| purchaseOrder.getParsedArrivalDate().before(startDate))) {
					isValid = false;
				}

				if (dateType.equals(ApplicationConstants.SHIPPED_DATE)
						&& (Objects.isNull(purchaseOrder.getParsedShippedDate())
								|| purchaseOrder.getParsedShippedDate().after(endDate)
								|| purchaseOrder.getParsedShippedDate().before(startDate))) {
					isValid = false;
				}

				if (!CollectionUtils.isEmpty(filters.getStatusList())
						&& !filters.getStatusList().contains(purchaseOrder.getStatus())) {
					isValid = false;
				}
				if (!CollectionUtils.isEmpty(filters.getShipFromIds())
						&& !filters.getShipFromIds().contains(purchaseOrder.getShipFromWarehouseId())) {
					isValid = false;
				}
				if (!CollectionUtils.isEmpty(filters.getShipToIds())
						&& !filters.getShipToIds().contains(purchaseOrder.getShipToWareHouseId())) {
					isValid = false;
				}
				if (!CollectionUtils.isEmpty(filters.getRoutingIds())
						&& !filters.getRoutingIds().contains(purchaseOrder.getRoutingId())) {
					isValid = false;
				}
				if (!CollectionUtils.isEmpty(filters.getVendorIds())
						&& !filters.getVendorIds().contains(purchaseOrder.getVendorId())) {
					isValid = false;
				}
				if (!CollectionUtils.isEmpty(filters.getCustomerIds())
						&& !filters.getCustomerIds().contains(purchaseOrder.getClientId())) {
					isValid = false;
				}
				if (!CollectionUtils.isEmpty(filters.getCarriers())
						&& !filters.getCarriers().contains(purchaseOrder.getCarrier())) {
					isValid = false;
				}
				if (!CollectionUtils.isEmpty(filters.getSupplierUsers())
						&& !filters.getStatusList().contains(purchaseOrder.getStatus())) {
					isValid = false;
				}

				if (Integer.valueOf(0).equals(member.getUnrestricted())) {
					List<CompanyRestrictions> companyRestrictions = restrictions.getCompanyRestrictions();
					if (!CollectionUtils.isEmpty(companyRestrictions)) {
						List<BigInteger> restrictedCompanies = null;
						if (securityContextUserDetails.isBuyer()) {
							restrictedCompanies = companyRestrictions.stream()
									.filter(rest -> "BS".equals(rest.getTransactionTypeDetails()))
									.map(CompanyRestrictions::getMemberCompanyId).collect(Collectors.toList());
						} else {
							restrictedCompanies = companyRestrictions.stream()
									.filter(rest -> "SR".equals(rest.getTransactionTypeDetails()))
									.map(CompanyRestrictions::getMemberCompanyId).collect(Collectors.toList());
						}
						if (!CollectionUtils.isEmpty(restrictedCompanies)
								&& !restrictedCompanies.contains(purchaseOrder.getShipToWareHouseId())) {
							isValid = false;
						}
					}
				}

				if (Integer.valueOf(0).equals(member.getUnrestrictedCatalog())) {
					List<CatalogRestrictions> catalogRestrictions = restrictions.getCatalogRestrictions();
					if (!CollectionUtils.isEmpty(catalogRestrictions) && securityContextUserDetails.isBuyer()) {
						List<BigInteger> restrictedCatalog = catalogRestrictions.stream()
								.map(CatalogRestrictions::getMemberCompanyCatalogId).collect(Collectors.toList());
						List<BigInteger> items = purchaseOrder.getPurchaseOrderItems().stream()
								.filter(item -> Objects.nonNull(item.getProductId()))
								.map(PurchaseOrderItem::getProductId).collect(Collectors.toList());
						if (!CollectionUtils.isEmpty(restrictedCatalog) && !CollectionUtils.isEmpty(items)) {
							for (int j = 0; j < items.size(); j++) {
								if (restrictedCatalog.contains(items.get(j))) {
									isValid = false;
									break;
								}
							}
						}
					}
				}

				if (Integer.valueOf(0).equals(member.getUnrestrictedWh())) {
					List<LocationRestrictions> locationRestrictions = restrictions.getLocationRestrictions();
					List<BigInteger> restrictedLocations = null;
					if (!CollectionUtils.isEmpty(locationRestrictions)) {
						if (securityContextUserDetails.isBuyer()) {
							restrictedLocations = locationRestrictions.stream()
									.filter(rest -> "WR".equals(rest.getTransactionTypeDetails()))
									.map(LocationRestrictions::getMemberCompanyLocationId).collect(Collectors.toList());
							if (!CollectionUtils.isEmpty(restrictedLocations)
									&& !restrictedLocations.contains(purchaseOrder.getShipToWareHouseId())) {
								isValid = false;
							}
						} else {
							restrictedLocations = locationRestrictions.stream()
									.filter(rest -> "WC".equals(rest.getTransactionTypeDetails()))
									.map(LocationRestrictions::getMemberCompanyLocationId).collect(Collectors.toList());
						}
						if (!CollectionUtils.isEmpty(restrictedLocations)
								&& !restrictedLocations.contains(purchaseOrder.getShipFromWarehouseId())) {
							isValid = false;
						}
					}
				}

				if (Integer.valueOf(0).equals(member.getUnRestrictedAWH())) {
					List<LocationRestrictions> locationRestrictions = restrictions.getLocationRestrictions();
					if (!CollectionUtils.isEmpty(locationRestrictions) && securityContextUserDetails.isBuyer()) {
						List<BigInteger> restrictedLocations = locationRestrictions.stream()
								.filter(rest -> "AWR".equals(rest.getTransactionTypeDetails()))
								.map(LocationRestrictions::getMemberCompanyLocationId).collect(Collectors.toList());
						if (!CollectionUtils.isEmpty(restrictedLocations)
								&& !restrictedLocations.contains(purchaseOrder.getShipToWareHouseId())) {
							isValid = false;
						}
					}
				}

				if (Integer.valueOf(0).equals(member.getUnrestrictedComm())) {
					List<CategoryRestrictions> categoryRestrictions = restrictions.getCategoryRestrictions();
					if (!CollectionUtils.isEmpty(categoryRestrictions) && securityContextUserDetails.isBuyer()) {
						List<String> retrictedCategories = categoryRestrictions.stream()
								.map(CategoryRestrictions::getCategoryName).collect(Collectors.toList());
						
						List<String> categories = products.stream().map(Product::getCategoryName).collect(Collectors.toList());
						
						if (!CollectionUtils.isEmpty(retrictedCategories) && !CollectionUtils.isEmpty(categories)) {
							for (int j = 0; j < categories.size(); j++) {
								if (retrictedCategories.contains(categories.get(j))) {
									isValid = false;
									break;
								}
							}
						}
					}
				}

				if (isValid) {
					if (pageable.getPageSize() == purchaseOrders.size()) {
						break;
					}
					purchaseOrders.add(purchaseOrder);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return purchaseOrders;
	}

	private String generateUserKey(FilterDataSet filters, UserPreference userPreference, Pageable pageable) {
		String date = "-14|14";
		String dateType = null;
		if (null != userPreference) {
			date = userPreference.getPrefValue();
			String key = userPreference.getPrefName();
			if (key.contains("CreateDate")) {
				dateType = ApplicationConstants.PO_SUBMISSION_DATE.substring(0, 1);
			} else if (key.contains("ArrivalDate")) {
				dateType = ApplicationConstants.ARRIVAL_DATE.substring(0, 1);
			} else if (key.contains("ShipDate")) {
				dateType = ApplicationConstants.SHIPPED_DATE.substring(0, 1);
			} else if (key.contains("InvoiceDate")) {
				dateType = ApplicationConstants.INVOICE_DATE.substring(0, 1);
			}
		}
		String keym = String.format("CID:%s:SN=%s:D=%s:DT=%s:P=%s:PS=%s:UI=%s", securityContextUserDetails.getOrgId(),
				filters.getScreenName().substring(0, 1), date, dateType, pageable.getPageNumber(),
				pageable.getPageSize(), securityContextUserDetails.getUserDetails().getONBUser().getOmeUserId());

		if (!CollectionUtils.isEmpty(filters.getStatusList())) {
			String joinedStr = filters.getStatusList().stream().map(i -> i.toString()).collect(Collectors.joining(","));
			keym += ":SL=" + joinedStr;
		}
		if (!CollectionUtils.isEmpty(filters.getShipFromIds())) {
			String joinedStr = filters.getShipFromIds().stream().map(i -> i.toString())
					.collect(Collectors.joining(","));
			keym += ":SF=" + joinedStr;
		}
		if (!CollectionUtils.isEmpty(filters.getShipToIds())) {
			String joinedStr = filters.getShipToIds().stream().map(i -> i.toString()).collect(Collectors.joining(","));
			keym += ":ST=" + joinedStr;
		}
		if (!CollectionUtils.isEmpty(filters.getRoutingIds())) {
			String joinedStr = filters.getRoutingIds().stream().map(i -> i.toString()).collect(Collectors.joining(","));
			keym += ":RO=" + joinedStr;
		}
		if (!CollectionUtils.isEmpty(filters.getVendorIds())) {
			String joinedStr = filters.getVendorIds().stream().map(i -> i.toString()).collect(Collectors.joining(","));
			keym += ":VS=" + joinedStr;
		}
		if (!CollectionUtils.isEmpty(filters.getCustomerIds())) {
			String joinedStr = filters.getCustomerIds().stream().map(i -> i.toString())
					.collect(Collectors.joining(","));
			keym += ":CS=" + joinedStr;
		}
		if (!CollectionUtils.isEmpty(filters.getCarriers())) {
			String joinedStr = filters.getCarriers().stream().map(i -> i.toString()).collect(Collectors.joining(","));
			keym += ":CR=" + joinedStr;
		}
		if (!CollectionUtils.isEmpty(filters.getSupplierUsers())) {
			String joinedStr = filters.getSupplierUsers().stream().map(i -> i.toString())
					.collect(Collectors.joining(","));
			keym += ":SU=" + joinedStr;
		}
		return keym;
	}

	public Member getUserRestrictions() {
		Member member = purchaseOrderDAO.getUser();
		Map<String, String> userMap = new HashMap<>();
		UserRestrictionsBuilder userRestrictionsPortalPBuilder = UserRestrictions.builder();
		UserRestrictionsBuilder userRestrictionsPortalMBuilder = UserRestrictions.builder();

		if (Integer.valueOf(0).equals(member.getUnrestrictedCatalog())) {
			List<CatalogRestrictions> catalogRestrictions = purchaseOrderDAO.getUserCatalogRestrictions();
			if (!CollectionUtils.isEmpty(catalogRestrictions)) {
				List<CatalogRestrictions> pPortalCatalogRestrictions = catalogRestrictions.stream()
						.filter(restriction -> "P".equalsIgnoreCase(restriction.getPortalChar()))
						.collect(Collectors.toList());
				userRestrictionsPortalPBuilder.catalogRestrictions(pPortalCatalogRestrictions);
				List<CatalogRestrictions> mPortalCatalogRestrictions = catalogRestrictions.stream()
						.filter(restriction -> "M".equalsIgnoreCase(restriction.getPortalChar()))
						.collect(Collectors.toList());
				userRestrictionsPortalMBuilder.catalogRestrictions(mPortalCatalogRestrictions);
			}
		}

		if (Integer.valueOf(0).equals(member.getUnrestrictedWh())) {
			List<LocationRestrictions> locationRestrictions = purchaseOrderDAO.getUserLocationRestrictions();
			if (!CollectionUtils.isEmpty(locationRestrictions)) {
				List<LocationRestrictions> pPortalLocationRestrictions = locationRestrictions.stream()
						.filter(restriction -> "P".equalsIgnoreCase(restriction.getPortalChar()))
						.collect(Collectors.toList());
				userRestrictionsPortalPBuilder.locationRestrictions(pPortalLocationRestrictions);
				List<LocationRestrictions> mPortalLocationRestrictions = locationRestrictions.stream()
						.filter(restriction -> "M".equalsIgnoreCase(restriction.getPortalChar()))
						.collect(Collectors.toList());
				userRestrictionsPortalMBuilder.locationRestrictions(mPortalLocationRestrictions);
			}
		}

		if (Integer.valueOf(0).equals(member.getUnrestricted())) {
			List<CompanyRestrictions> catalogRestrictions = purchaseOrderDAO.getUserCompanyRestrictions();
			if (!CollectionUtils.isEmpty(catalogRestrictions)) {
				List<CompanyRestrictions> pPortalTransactionTypeRestrictions = catalogRestrictions.stream()
						.filter(restriction -> "P".equalsIgnoreCase(restriction.getPortalChar()))
						.collect(Collectors.toList());
				userRestrictionsPortalPBuilder.companyRestrictions(pPortalTransactionTypeRestrictions);
				List<CompanyRestrictions> mPortalTransactionTypeRestrictions = catalogRestrictions.stream()
						.filter(restriction -> "M".equalsIgnoreCase(restriction.getPortalChar()))
						.collect(Collectors.toList());
				userRestrictionsPortalMBuilder.companyRestrictions(mPortalTransactionTypeRestrictions);
			}
		}

		if (Integer.valueOf(0).equals(member.getUnrestrictedComm())) {
			List<CategoryRestrictions> catalogRestrictions = purchaseOrderDAO.getCategoryRestrictions();
			if (!CollectionUtils.isEmpty(catalogRestrictions)) {
				List<CategoryRestrictions> pPortalCategoryRestrictions = catalogRestrictions.stream()
						.filter(restriction -> "P".equalsIgnoreCase(restriction.getPortalChar()))
						.collect(Collectors.toList());
				userRestrictionsPortalPBuilder.categoryRestrictions(pPortalCategoryRestrictions);
				List<CategoryRestrictions> mPortalCategoryRestrictions = catalogRestrictions.stream()
						.filter(restriction -> "M".equalsIgnoreCase(restriction.getPortalChar()))
						.collect(Collectors.toList());
				userRestrictionsPortalMBuilder.categoryRestrictions(mPortalCategoryRestrictions);
			}
		}

		try {
			userMap.put("userInfo", objectMapper.writeValueAsString(member));
			userMap.put("restrictions:P", objectMapper.writeValueAsString(userRestrictionsPortalPBuilder.build()));
			userMap.put("restrictions:M", objectMapper.writeValueAsString(userRestrictionsPortalMBuilder.build()));
		} catch (Exception e) {

		}

		Jedis jedis = jedisPool.getResource();
		jedis.hmset(getEnvSpecificNamespaces("USER") + ":" + securityContextUserDetails.getUserId(), userMap);

		return member;
	}
}
