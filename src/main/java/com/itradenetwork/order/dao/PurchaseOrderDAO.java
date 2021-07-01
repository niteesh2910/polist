package com.itradenetwork.order.dao;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

import com.itradenetwork.auth.security.SecurityContextUserDetails;
import com.itradenetwork.framework.dao.BaseDAO;
import com.itradenetwork.framework.dto.CatalogRestrictions;
import com.itradenetwork.framework.dto.CategoryRestrictions;
import com.itradenetwork.framework.dto.CompanyRestrictions;
import com.itradenetwork.framework.dto.LocationRestrictions;
import com.itradenetwork.framework.dto.Member;
import com.itradenetwork.framework.entity.PurchaseOrder;
import com.itradenetwork.framework.entity.PurchaseOrderItem;
import com.itradenetwork.framework.utils.FilterAndInclusionSpecifications;
import com.itradenetwork.framework.utils.OracleQueryBuilder;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
@AllArgsConstructor
public class PurchaseOrderDAO extends BaseDAO {

	private SecurityContextUserDetails securityContextUserDetails;

	private static final String MEMBER_ID = "memberId";

	private static final String GET_LOCATION_RESTRICTIONS = "SELECT MEMBERCOMPANYLOCATIONID memberCompanyLocationId, TRANSACTIONTYPE transactionType, "
			+ " TRANSACTIONTYPEDETAILS transactionTypeDetails, PORTALCHAR portalChar FROM MEMBER_MEMBERCOMPANYLOCATION WHERE MEMBERID = :memberId";

	private static final String GET_CATALOG_RESTRICTIONS = "SELECT MEMBERCOMPANYCATALOGID memberCompanyCatalogId,"
			+ " PORTALCHAR portalChar FROM MEMBER_MEMBERCOMPANYCATALOG WHERE MEMBERID = :memberId";

	private static final String GET_CATEGORY_RESTRICTIONS = "SELECT CATEGORYNAME categoryName, PORTALCHAR portalChar FROM MEMBERCOMMODITY WHERE MEMBERID = :memberId";

	private static final String GET_COMPANY_RESTRICTIONS = "SELECT MEMBERCOMPANYID memberCompanyId, TRANSACTIONTYPE transactionType, TRANSACTIONTYPEDETAILS transactionTypeDetails,"
			+ " PORTALCHAR portalChar FROM MEMBER_MEMBERCOMPANY WHERE MEMBERID = :memberId";

	private static final String GET_USER = "SELECT MEMBERID memberId, MEMBERCOMPANYID memberCompanyId, MEMBERNAME memberName, FIRSTNAME firstName, LASTNAME lastName, MAXROWS maxRows,"
			+ " REFRESHRATE refreshRate, PREFERREDPUOM preferredPUOM, PREFERREDUOM preferredUOM, PREFERREDUOMCODE preferredUOMCode, SHOWCONTAINER showContainer, "
			+ " SHOWREGION showRegion, SHOWGRADE showGrade, SHOWGROWTHPROCESS showGrowthProcess, SHOWGROWTHTYPE showGrowthType, SHOWLABEL showLabel, ALLOWMENUHIDING allowMenuHiding,"
			+ " EVENTS_SEND eventsSend, EVENTS_NOTIFY eventsNotify, UNRESTRICTED unrestricted, UNRESTRICTED_WH unrestrictedWh, "
			+ " UNRESTRICTED_COMM unrestrictedComm, UNRESTRICTED_CATALOG unrestrictedCatalog, CURRENTPORTAL currentPortal, STATUSNAME statusName, POFILTERID poFilterID, DEFAULTMEMBERCOMPANYID defaultMemberCompanyId,"
			+ " MENUID menuId, PO_PREFIX poPrefix, PREFERREDLANGUAGE preferredLanguage, "
			+ " TIMEZONE timeZone, SHOWCUPC showCUPC, SHOWHS showHS, SHOWGTIN showGTIN, SHOWSIZE showSize, SHOWPACKCOUNT showPackCount, SHOWRESTRICTEDFOR showRestrictedFor, HASPGS hasPGS,"
			+ " HASMCCCONFIG hasMCCConfig, PREFERREDPUOM preferredPUOM, UNRESTRICTED_DIST unrestrictedDist, UNRESTRICTED_AWH unRestrictedAWH, "
			+ " EMAIL email, NSUNIQUEID nsUniqueId, PHONE phone FROM MEMBER WHERE MEMBERID = :memberId";

	private static final String GET_PO_ID_ITN_LOG = "select distinct decode(tablename,'PURCHASEORDER2',lo.pk_id,'PURCHASEORDER2_CATALOGENTRY',fk_id1) poid from ITNAUDITLOG lo"
			+ " where AUDITDATETIME > sysdate - interval '10' SECOND and tablename in ('PURCHASEORDER2','PURCHASEORDER2_CATALOGENTRY')";

	private static final String GET_PO = " SELECT P.PURCHASEORDERID Id,P.LOADNUMBER loadNumber, to_char(to_date(P.DATEREQUIRED,'DD-MM-YY'), 'MM/DD/YYYY') arrivalDate, P.ROUTINGTYPE, P.CURRENCY currency, P.PURCHASEORDERNUMBER poNumber, "
			+ "  P.DATESUBMITTED poSubmissionDate, P.DATEDELIVERED receivedDate, P.BUYERMEMBERCOMPANYID clientId, to_char(to_date(P.DATESHIPPING,'DD-MM-YY'), 'MM/DD/YYYY') shippedDate,  P.STATUSNAME statusName, "
			+ "  P.SELLERMEMBERCOMPANYID vendorId, P.DELIVEREDLOCID shipToWareHouseId, P.PICKUPLOCATIONID shipFromWareHouseId, "
			+ "  P.POLINK poLink, P.LOADID loadId FROM PURCHASEORDER2 P ";

	private static final String GET_PO_LINEITEMS = "SELECT PURCHASEORDER_CATALOGENTRYID id, PURCHASEORDERID purchaseorderId, INVOICEID invoiceId, BUYERMEMBERCATALOGENTRYID productId, SELLERMEMBERCATALOGENTRYID vendorPoroductId,"
			+ " PICKUPNUMBER pickupNumber, PRICE price, QUANTITY quantity, BUYERADJUSTEDPRICE buyerAdjustedPrice, ORIGINALPRICE originalPrice ,ORIGINALQUANTITY originalQuantity,"
			+ " BUYERADJUSTEDQUANTITY buyerAdjustedQuantity, SELLERADJUSTEDPRICE sellerAdjustedPrice, SELLERADJUSTEDQUANTITY sellerAdjustedQuantity, STATUSNAME statusName, ADDEDBY addedBy, SELLERADJUSTED, BUYERADJUSTED, ISADJUSTED, FREIGHT,"
			+ " DELIVERYLOCATIONID deliveryLocationId, CANCELFLAG cancelFlag, MOVEDFLAG, DATEDELIVERED dateDelivered,"
			+ " QUANTITYRECEIVED quantityReceived, SHIPPEDQUANTITY quantityShipped, DATECANCELMOVE canceledDate, CANCELMOVEBYUSER canceledByMemberId, ISWEIGHT, SELLERWEIGHT,UOM,PUOM, BUYERWEIGHT, WEIGHTFLAG, SHIPPEDWEIGHT shippedWeight, RECEIVEDWEIGHT receivedWeight, CANCELBY, SELLERADJUSTEDPRICE_TOTAL, DATEMARKEDRECEIVED, "
			+ " CANCELREQUESTED, BUYERADJUSTEDWEIGHT, SELLERADJUSTEDWEIGHT, BUYERNETWEIGHT, SELLERNETWEIGHT, SHIPPEDCASES, RECEIVEDCASES, COOL, INVOICETOTAL from PURCHASEORDER2_CATALOGENTRY WHERE PURCHASEORDERID IN (:poids)";

	public List<PurchaseOrder> getPurchaseOrders(List<BigInteger> ids) {
		List<PurchaseOrder> pos = null;
		try {
			pos = namedParameterJdbcTemplateEnterprise.query(GET_PO + " WHERE P.PURCHASEORDERID IN (:idList)",
					Collections.singletonMap("idList", ids),
					new BeanPropertyRowMapper<PurchaseOrder>(PurchaseOrder.class));
		} catch (EmptyResultDataAccessException e) {
			log.error("PurchaseOrderDAO.getPurchaseOrders error occured while getting pos - {}", e.getMessage(), e);
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

	public Map<BigInteger, List<PurchaseOrderItem>> getPurchaseOrderItems(List<BigInteger> ids) {
		Map<BigInteger, List<PurchaseOrderItem>> items = null;
		try {
			items = namedParameterJdbcTemplateEnterprise.query(GET_PO_LINEITEMS, Collections.singletonMap("poids", ids),
					(ResultSet rs) -> {
						Map<BigInteger, List<PurchaseOrderItem>> results = new HashMap<>();
						BeanPropertyRowMapper<PurchaseOrderItem> row = new BeanPropertyRowMapper<>(
								PurchaseOrderItem.class);
						while (rs.next()) {
							BigInteger poId = BigInteger.valueOf(rs.getLong("purchaseorderId"));
							results.computeIfAbsent(poId, k -> new ArrayList<>());
							results.get(poId).add(row.mapRow(rs, 0));
						}
						return results;
					});
		} catch (EmptyResultDataAccessException e) {
			log.error("PurchaseOrderDAO.getPurchaseOrderItems error occured while getting po items - {}",
					e.getMessage(), e);
		}
		return items;
	}

	public Page<PurchaseOrder> getPagedPurchaseOrders(Pageable pageable, String whereClause, Map<String, Object> params,
			FilterAndInclusionSpecifications filterAndInclude) {
		log.info("PurchaseOrderDAO.getPagedPurchaseOrders starts");
		StringBuilder query = new StringBuilder(GET_PO).append(whereClause);

		String pagedQuery = new OracleQueryBuilder(query.toString()).filterNamedParameter(filterAndInclude, "P")
				.namedParameterSetValues(filterAndInclude, params).with(pageable, "P").toString();
		log.info("PurchaseOrderDAO.getPagedPurchaseOrders query - {}, params - {}", pagedQuery, params);

		Long totalCount = 0l;
		List<PurchaseOrder> resultsList = null;
		final String countQuery = " select count(1) from (" + pagedQuery + ")";
		try {
			resultsList = namedParameterJdbcTemplateEnterprise.query(pagedQuery, params,
					new BeanPropertyRowMapper<PurchaseOrder>(PurchaseOrder.class));
			totalCount = namedParameterJdbcTemplateEnterprise.queryForObject(countQuery, params, Long.class);
		} catch (DataAccessException e) {
			log.info("PurchaseOrderDAO.getPagedPurchaseOrders no orders found for selected criteria");
		}
		log.info("PurchaseOrderDAO.getPagedPurchaseOrders exit with totalCount - {}", totalCount);
		return new PageImpl<>(CollectionUtils.isEmpty(resultsList) ? Collections.emptyList() : resultsList, pageable,
				totalCount);
	}

	public Member getUser() {
		Member member = null;
		try {
			member = namedParameterJdbcTemplateEnterprise.queryForObject(GET_USER,
					Collections.singletonMap(MEMBER_ID, securityContextUserDetails.getUserId()),
					new BeanPropertyRowMapper<Member>(Member.class));
		} catch (EmptyResultDataAccessException e) {
			log.error("PurchaseOrderDAO.getUser error occured while getting user information - {}", e.getMessage(), e);
		}
		return member;
	}

	public List<CompanyRestrictions> getUserCompanyRestrictions() {
		List<CompanyRestrictions> pos = null;
		try {
			pos = namedParameterJdbcTemplateEnterprise.query(GET_COMPANY_RESTRICTIONS,
					Collections.singletonMap(MEMBER_ID, securityContextUserDetails.getUserId()),
					new BeanPropertyRowMapper<CompanyRestrictions>(CompanyRestrictions.class));
		} catch (EmptyResultDataAccessException e) {
			log.error(
					"PurchaseOrderDAO.getUserCompanyRestrictions error occured while getting transaction type restrictions - {}",
					e.getMessage(), e);
		}
		return pos;
	}

	public List<LocationRestrictions> getUserLocationRestrictions() {
		List<LocationRestrictions> pos = null;
		try {
			pos = namedParameterJdbcTemplateEnterprise.query(GET_LOCATION_RESTRICTIONS,
					Collections.singletonMap(MEMBER_ID, securityContextUserDetails.getUserId()),
					new BeanPropertyRowMapper<LocationRestrictions>(LocationRestrictions.class));
		} catch (EmptyResultDataAccessException e) {
			log.error(
					"PurchaseOrderDAO.getUserLocationRestrictions error occured while getting location restrictions - {}",
					e.getMessage(), e);
		}
		return pos;
	}

	public List<CatalogRestrictions> getUserCatalogRestrictions() {
		List<CatalogRestrictions> pos = null;
		try {
			pos = namedParameterJdbcTemplateEnterprise.query(GET_CATALOG_RESTRICTIONS,
					Collections.singletonMap(MEMBER_ID, securityContextUserDetails.getUserId()),
					new BeanPropertyRowMapper<CatalogRestrictions>(CatalogRestrictions.class));
		} catch (EmptyResultDataAccessException e) {
			log.error(
					"PurchaseOrderDAO.getUserCatalogRestrictions error occured while getting catalog restrictions - {}",
					e.getMessage(), e);
		}
		return pos;
	}

	public List<CategoryRestrictions> getCategoryRestrictions() {
		List<CategoryRestrictions> pos = null;
		try {
			pos = namedParameterJdbcTemplateEnterprise.query(GET_CATEGORY_RESTRICTIONS,
					Collections.singletonMap(MEMBER_ID, securityContextUserDetails.getUserId()),
					new BeanPropertyRowMapper<CategoryRestrictions>(CategoryRestrictions.class));
		} catch (EmptyResultDataAccessException e) {
			log.error("PurchaseOrderDAO.getCategoryRestrictions error occured while getting category restrictions - {}",
					e.getMessage(), e);
		}
		return pos;
	}

}
