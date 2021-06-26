package com.itradenetwork.order.dao;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import com.itradenetwork.framework.dao.BaseDAO;
import com.itradenetwork.framework.entity.PurchaseOrder;
import com.itradenetwork.framework.utils.FilterAndInclusionSpecifications;
import com.itradenetwork.framework.utils.QueryBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
public class PurchaseOrderDAO extends BaseDAO {

	private static final String GET_PO_ID_ITN_LOG = "select distinct decode(tablename,'PURCHASEORDER2',lo.pk_id,'PURCHASEORDER2_CATALOGENTRY',fk_id1) poid from itnauditlog lo"
			+ " where AUDITDATETIME > sysdate - interval '10' DAY and tablename in ('PURCHASEORDER2','PURCHASEORDER2_CATALOGENTRY')";

	private static final String GET_PO = " SELECT PO.PURCHASEORDERID Id,PO.LOADNUMBER loadNumber, PO.DATEREQUIRED arrivalDate, PO.ROUTINGTYPE, PO.CURRENCY currency, PO.PURCHASEORDERNUMBER poNumber, "
			+ "  PO.DATESUBMITTED poSubmissionDate, PO.DATEDELIVERED receivedDate,  PO.BUYERMEMBERCOMPANYID clientId, PO.DATESHIPPING shippedDate,  PO.STATUSNAME statusName, "
			+ "  PO.SELLERMEMBERCOMPANYID vendorId, PO.DELIVEREDLOCID shipToWareHouseId, PO.PICKUPLOCATIONID shipFromWareHouseId, "
			+ "  PO.POLINK poLink, PO.LOADID loadId FROM PURCHASEORDER2 PO WHERE PO.PURCHASEORDERID IN (:idList)";

	public List<PurchaseOrder> getPurchaseOrders(List<BigInteger> ids) {
		List<PurchaseOrder> pos = null;
		try {
			pos = namedParameterJdbcTemplateEnterprise.query(GET_PO, Collections.singletonMap("idList", ids),
					new BeanPropertyRowMapper<PurchaseOrder>(PurchaseOrder.class));
		} catch (EmptyResultDataAccessException e) {
			log.error("PurchaseOrderDAO.invokeITNLogs getPurchaseOrders occured while getting pos - {}", e.getMessage(),
					e);
		}
		return pos;
	}

	public List<BigInteger> getItnLogPurchaseOrders() {
		List<BigInteger> pos = null;
		try {
			pos = jdbcTemplateEnterprise.queryForList(GET_PO_ID_ITN_LOG, BigInteger.class);
		} catch (EmptyResultDataAccessException e) {
			log.error("PurchaseOrderDAO.getItnLogPurchaseOrders error occured while getting po ids - {}",
					e.getMessage(), e);
		}
		return pos;
	}

	public Page<PurchaseOrder> getPagedPurchaseOrders(Pageable pageable, String whereClause, Map<String, Object> params,
			FilterAndInclusionSpecifications filterAndInclude) {
		log.info("PurchaseOrderDAO.getPagedPurchaseOrders starts");
		StringBuilder query = new StringBuilder(GET_PO).append(whereClause);

		String pagedQuery = new QueryBuilder(query.toString()).filterNamedParameter(filterAndInclude, "PO", false)
				.namedParameterSetValues(filterAndInclude, params).with(pageable, "PO").toString();
		log.info("PurchaseOrderDAO.getPagedPurchaseOrders query - {}, params - {}", pagedQuery, params);

		Long totalCount = 0l;
		List<PurchaseOrder> resultsList = null;
		String countQuery = new QueryBuilder(pagedQuery, true, "ID", "P").toString();
		try {
			resultsList = namedParameterJdbcTemplate.query(pagedQuery, params,
					new BeanPropertyRowMapper<PurchaseOrder>(PurchaseOrder.class));
			totalCount = namedParameterJdbcTemplate.queryForObject(countQuery, params, Long.class);
		} catch (DataAccessException e) {
			log.info("PurchaseOrderDAO.getPagedPurchaseOrders no orders found for selected criteria");
		}
		log.info("PurchaseOrderDAO.getPagedPurchaseOrders exit with totalCount - {}", totalCount);
		return new PageImpl<>(CollectionUtils.isEmpty(resultsList) ? Collections.emptyList() : resultsList, pageable,
				totalCount);
	}
}
