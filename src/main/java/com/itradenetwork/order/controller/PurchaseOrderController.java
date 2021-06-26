package com.itradenetwork.order.controller;

import java.math.BigInteger;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.itradenetwork.framework.config.RestControllerHandler;
import com.itradenetwork.framework.entity.FilterDataSet;
import com.itradenetwork.framework.entity.PurchaseOrder;
import com.itradenetwork.framework.utils.ApiConstant;
import com.itradenetwork.framework.utils.FilterAndInclusionSpecifications;
import com.itradenetwork.order.cron.ITNAuditLogCron;
import com.itradenetwork.order.service.PurchaseOrderService;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Controller
@AllArgsConstructor
@RestControllerHandler
@RequestMapping(ApiConstant.API_BASEPATH_V1 + "/orders")
public class PurchaseOrderController {

	private ITNAuditLogCron itnAuditLogCron;
	private PurchaseOrderService purchaseOrderService;
	
	@GetMapping
	public ResponseEntity<Page<PurchaseOrder>> getAll(Pageable pageable,
			@RequestParam(value = "search", required = false) String search,
			@RequestParam(value = "searchOr", required = false) String searchOr,
			@RequestParam(value = "include", required = false) String[] include,
			@RequestParam(value = "viewName", required = false) String viewName,
			@RequestParam(value = "screenName", required = false) String screenName,
			@RequestParam(value = "carriers", required = false) List<String> carriers,
			@RequestParam(value = "buyerUsers", required = false) List<String> buyerUsers,
			@RequestParam(value = "statusList", required = false) List<Integer> statusList,
			@RequestParam(value = "shipToIds", required = false) List<BigInteger> shipToIds,
			@RequestParam(value = "vendorIds", required = false) List<BigInteger> vendorIds,
			@RequestParam(value = "routingIds", required = false) List<BigInteger> routingIds,
			@RequestParam(value = "isFilterApplied", required = false) boolean isFilterApplied,
			@RequestParam(value = "shipFromIds", required = false) List<BigInteger> shipFromIds,
			@RequestParam(value = "customerIds", required = false) List<BigInteger> customerIds,
			@RequestParam(value = "supplierUsers", required = false) List<String> supplierUsers,
			@RequestParam(value = "companyType", required = false) String companyType) {
		log.info("PurchaseOrderController.getAll pageable - {}", pageable);
		FilterDataSet filters = FilterDataSet.builder().include(include).statusList(statusList).screenName(screenName)
				.viewName(viewName).shipFromIds(shipFromIds).shipToIds(shipToIds).routingIds(routingIds)
				.buyerUsers(buyerUsers).vendorIds(vendorIds).customerIds(customerIds).carriers(carriers)
				.supplierUsers(supplierUsers).isFilterApplied(isFilterApplied).build();
		log.info("PurchaseOrderController.getAll filters - {}", filters);
		FilterAndInclusionSpecifications filterAndInclude = new FilterAndInclusionSpecifications();
		// excluding include as we are sending all child objects
		filterAndInclude = filterAndInclude.build(search, include, searchOr);
		if (!filterAndInclude.isValidSearch(search) || !filterAndInclude.isValidSearchOr(searchOr)) {
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
		}
		Page<PurchaseOrder> purchaseOrders = purchaseOrderService.getOrders(pageable, filterAndInclude, filters);
		log.info("PurchaseOrderController.getAll exit PurchaseOrder size - {}", purchaseOrders);
		return new ResponseEntity<>(purchaseOrders, HttpStatus.OK);
	}
	
	@GetMapping("/cron")
	public ResponseEntity<Boolean> getAll() {
		itnAuditLogCron.invokeITNLogs();
		return new ResponseEntity<>(true, HttpStatus.OK);
	}
}
