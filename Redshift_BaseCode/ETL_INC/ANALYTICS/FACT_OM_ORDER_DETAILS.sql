/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/
begin;

delete from bec_dwh.FACT_OM_ORDER_DETAILS
where exists 
(
select 1
FROM 
bec_ods.OE_ORDER_LINES_ALL OOL
,bec_ods.OE_ORDER_HEADERS_ALL  OOH
,bec_dwh.DIM_CUSTOMER_DETAILS  BILL
,bec_dwh.DIM_CUSTOMER_DETAILS  SHIP
,bec_dwh.DIM_CUSTOMER_DETAILS  LINE_SHIP
,bec_ods.WSH_DELIVERY_DETAILS  WDD 
,bec_ods.HZ_CUST_ACCOUNTS  HCA
,bec_ods.HZ_PARTIES  HP
WHERE 1=1
AND OOH.HEADER_ID  = OOL.HEADER_ID
AND OOH.INVOICE_TO_ORG_ID =BILL.SITE_USE_ID(+)
AND BILL.SITE_USE_CODE(+) = 'BILL_TO'
AND OOH.SHIP_TO_ORG_ID =SHIP.SITE_USE_ID(+)
AND SHIP.SITE_USE_CODE(+) = 'SHIP_TO'
AND OOL.SHIP_TO_ORG_ID =LINE_SHIP.SITE_USE_ID(+)
AND LINE_SHIP.SITE_USE_CODE(+) = 'SHIP_TO'
AND OOH.SOLD_TO_ORG_ID    = HCA.CUST_ACCOUNT_ID(+)
AND HCA.PARTY_ID          = HP.PARTY_ID(+)
AND OOL.HEADER_ID = WDD.SOURCE_HEADER_ID(+) 
AND OOL.LINE_ID  = WDD.SOURCE_LINE_ID (+)   
and 
(OOH.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_order_details' and batch_name = 'om')
OR
OOL.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_order_details' and batch_name = 'om')
)
and fact_om_order_details.dw_load_id =
(
select
	system_id
from
	bec_etl_ctrl.etlsourceappid
where
	source_system = 'EBS'
)
||'-'|| nvl(OOH.HEADER_ID, 0) 
||'-'|| nvl(OOL.LINE_ID, 0)
||'-'|| nvl(OOH.ORG_ID, 0)
);
commit;

insert into bec_dwh.fact_om_order_details
(
select 
invoice_to_org_id
,ship_to_org_id
,sold_to_org_id
,org_id
,header_id
,order_number
,cust_po_number
,customer_name
,customer_number
,order_type_id
,line_price_list_id
,line_id
,line_number
,currency
,conversion_rate
,header_status_code
,line_status_code
,order_source_id
,order_source_reference
,ordered_date
,ordered_quantity
,book_amount
,bill_to_customer_name
,bill_to_customer_number
,ship_to_customer_name
,ship_to_customer_number
,ship_to_addr_line1
,ship_to_addr_line2
,ship_to_addr_line5
,city
,county
,"state"
,country
,context_value
,contract_number
,contract_description
,gross_contract_value
,contract_milestones_amount
,contract_start_date
,contract_end_date
,header_dff7
,account_executive
,purchase_type
,"deposit%"
,project_costing_type
,shipping_terms
,freight_amount
,payment_term_id
,ordered_item
,quantity
,allocated_quantity
,picked_quantity
,open_quantity
,unit_selling_price
,unit_list_price
,extended_price
,pricing_date
,order_quantity_uom
,warehouse_id
,source_type
,line_type_id
,line_ship_addr1
,line_ship_addr2
,line_ship_addr5
,salesrep_id
,line_category_code
,booked_flag
,booked_month
,booked_year
,ool_inventory_item_id
,open_flag
,booked_date
,agreement_id
,request_date
,promise_date
,accounting_rule_id
,actual_shipment_date
,cancelled_flag
,created_by
,last_updated_by
,header_creation_date
,line_creation_date
,line_cust_po_number
,deliver_to_org_id
,fulfilled_flag
,shippable_flag
,fulfillment_date
,intmed_ship_to_org_id
,invoicing_rule_id
,item_type_code
,last_update_date
,return_reason_code
,schedule_arrival_date
,schedule_ship_date
,ool_ship_to_org_id
,sales_channel_code
,global_selling_price
,global_list_price
,book_quantity
,cancel_quantity
,ship_quantity
,invoiced_quantity
,cancel_amount
,ship_amount
,invoiced_amount
,met_crd_amount
,met_prd_amount
,discount_amount
,TAX_VALUE
,schedule_status_code
,BILL_TO_LOCATION
,sales_order_line_type
,BOOK_AMOUNT - (ship_amount) as ship_backlog_amount
,ship_amount - invoiced_amount as fin_backlog_amount
,'N' as is_deleted_flg,
(
select
	system_id
from
	bec_etl_ctrl.etlsourceappid
where
	source_system = 'EBS'
) as source_app_id,
(
select
	system_id
from
	bec_etl_ctrl.etlsourceappid
where
	source_system = 'EBS'
)
||'-'|| nvl(HEADER_ID, 0) 
||'-'|| nvl(LINE_ID, 0)
||'-'|| nvl(ORG_ID, 0) as dw_load_id,
getdate() as dw_insert_date,
getdate() as dw_update_date
from 
(SELECT  
 OOH.INVOICE_TO_ORG_ID
,OOH.SHIP_TO_ORG_ID
,OOH.SOLD_TO_ORG_ID
,OOH.ORG_ID
,OOH.HEADER_ID
,OOH.ORDER_NUMBER
,OOH.CUST_PO_NUMBER
,hp.party_name CUSTOMER_NAME
,HCA.ACCOUNT_NUMBER CUSTOMER_NUMBER
,OOH.ORDER_TYPE_ID
,OOL.price_list_id LINE_PRICE_LIST_ID
,OOL.LINE_ID
,OOL.LINE_NUMBER||'.'||SHIPMENT_NUMBER LINE_NUMBER
,OOH.TRANSACTIONAL_CURR_CODE CURRENCY
,OOH.CONVERSION_RATE
,OOH.FLOW_STATUS_CODE HEADER_STATUS_CODE
,OOL.FLOW_STATUS_CODE LINE_STATUS_CODE 
,OOH.ORDER_SOURCE_ID  --Need to get order source and description from Dimension table
,OOH.ORIG_SYS_DOCUMENT_REF ORDER_SOURCE_REFERENCE
,TRUNC (OOH.ordered_date) ORDERED_DATE
,OOL.ORDERED_QUANTITY
,(NVL (OOL.ordered_quantity, 0)   * NVL (OOL.unit_selling_price, 0) + NVL(OOL.TAX_VALUE,0) ) BOOK_AMOUNT
,BILL.PARTY_NAME BILL_TO_CUSTOMER_NAME
,BILL.ACCOUNT_NUMBER BILL_TO_CUSTOMER_NUMBER
,BILL.LOCATION BILL_TO_LOCATION
,ool.attribute4 as sales_order_line_type
,SHIP.PARTY_NAME SHIP_TO_CUSTOMER_NAME
,SHIP.ACCOUNT_NUMBER SHIP_TO_CUSTOMER_NUMBER 
,SHIP.ADDRESS1 SHIP_TO_ADDR_LINE1
,SHIP.ADDRESS2 SHIP_TO_ADDR_LINE2
,SHIP.ADDRESS5 SHIP_TO_ADDR_LINE5
,SHIP.CITY
,SHIP.COUNTY
,UPPER(SHIP.STATE) as STATE
,SHIP.COUNTRY
--Attributes
,OOH.CONTEXT       CONTEXT_VALUE
,OOH.ATTRIBUTE1    CONTRACT_NUMBER
,OOH.ATTRIBUTE2    CONTRACT_DESCRIPTION
,OOH.ATTRIBUTE3    GROSS_CONTRACT_VALUE
,OOH.ATTRIBUTE4    CONTRACT_MILESTONES_AMOUNT
,OOH.ATTRIBUTE5    CONTRACT_START_DATE
,OOH.ATTRIBUTE6    CONTRACT_END_DATE
,OOH.ATTRIBUTE7    HEADER_DFF7
,DECODE(case when OOH.attribute8 = ' India' then 'INDIA' else UPPER(OOH.ATTRIBUTE8) end,'INDIA', '999999', OOH.ATTRIBUTE8)   ACCOUNT_EXECUTIVE
,OOH.ATTRIBUTE9    PURCHASE_TYPE
,OOH.ATTRIBUTE10   "DEPOSIT%"
,OOH.ATTRIBUTE11   PROJECT_COSTING_TYPE
,OOH.ATTRIBUTE12   SHIPPING_TERMS
,OOH.ATTRIBUTE14   FREIGHT_AMOUNT
--,OOL.LINE_TYPE_ID
,OOH.PAYMENT_TERM_ID  --Need to get payment term from dimension table.
--,OOH.PRICE_LIST_ID HDR_PRICE_LIST_ID
,OOL.ORDERED_ITEM
--,MSI.DESCRIPTION ITEM_DESCRIPTION --Get item desc from dim_inv_master_items
,OOL.ORDERED_QUANTITY QUANTITY
,sum (DECODE (wdd.released_status,
                          'S', wdd.PICKED_QUANTITY)) ALLOCATED_QUANTITY
,sum (DECODE (wdd.released_status,
                          'Y',wdd.PICKED_QUANTITY)) PICKED_QUANTITY
,OOL.ORDERED_QUANTITY OPEN_QUANTITY 
,NVL (OOL.unit_selling_price, 0) UNIT_SELLING_PRICE
,NVL (OOL.unit_list_price, 0) UNIT_LIST_PRICE
,(NVL(OOL.ORDERED_QUANTITY,0) * NVL(OOL.UNIT_SELLING_PRICE,0)) EXTENDED_PRICE
,OOL.PRICING_DATE
,OOL.ORDER_QUANTITY_UOM
--,OOL.CREATION_DATE
--,OOL.REQUEST_DATE
--,OOL.SCHEDULE_SHIP_DATE
,OOL.SHIP_FROM_ORG_ID WAREHOUSE_ID --Need to get name from Dimension table.
,OOL.SOURCE_TYPE_CODE SOURCE_TYPE  -- Need to get meaning from dimension table where LOOKUP_TYPE = 'SOURCE_TYPE' and lookup code = source type.
,OOL.LINE_TYPE_ID   --Need to get line type from Dimension table.
,LINE_SHIP.ADDRESS1   LINE_SHIP_ADDR1
,LINE_SHIP.ADDRESS2   LINE_SHIP_ADDR2
,LINE_SHIP.ADDRESS5   LINE_SHIP_ADDR5
 --Document Type and Document Number can be retrived from DIM_Lookups and DIM_OM_PO_NUM tables.
,OOL.salesrep_id     -- Need to get NAME from DIM_OM_SALES_REPS dimension table.
,OOL.line_category_code 
,OOL.booked_flag
,TO_CHAR (OOH.booked_date, 'MON-RRRR') BOOKED_MONTH
,TO_CHAR (OOH.booked_date, 'RRRR') BOOKED_YEAR
,OOL.INVENTORY_ITEM_ID OOL_INVENTORY_ITEM_ID
,OOL.open_flag OPEN_FLAG
,OOH.booked_date BOOKED_DATE
,OOH.agreement_id
,TRUNC (OOL.request_date) REQUEST_DATE
,TRUNC (OOL.promise_date) promise_date
,OOL.accounting_rule_id 
,TRUNC (OOL.actual_shipment_date) actual_shipment_date
,OOL.cancelled_flag
,OOL.created_by
,OOL.last_updated_by
,TRUNC (OOh.creation_date) HEADER_CREATION_DATE
,TRUNC (OOL.creation_date) LINE_CREATION_DATE
,OOL.cust_po_number LINE_CUST_PO_NUMBER
,OOL.deliver_to_org_id 
,OOL.fulfilled_flag
,OOL.shippable_flag
,TRUNC (OOL.fulfillment_date) FULFILLMENT_DATE
,OOL.intmed_ship_to_org_id 
,OOL.invoicing_rule_id 
,OOL.item_type_code
,TRUNC (OOL.last_update_date) LAST_UPDATE_DATE
,OOL.return_reason_code
,TRUNC (OOL.schedule_arrival_date) schedule_arrival_date
,TRUNC (OOL.schedule_ship_date) schedule_ship_date
,OOL.ship_to_org_id OOL_SHIP_TO_ORG_ID
,OOH.sales_channel_code
,ROUND (NVL (OOL.unit_selling_price, 0), 2) GLOBAL_SELLING_PRICE
,ROUND (NVL (OOL.unit_list_price, 0), 2) GLOBAL_LIST_PRICE
,NVL (OOL.ordered_quantity, 0) BOOK_QUANTITY
,NVL (OOL.cancelled_quantity, 0) CANCEL_QUANTITY
,NVL (OOL.shipped_quantity, 0) SHIP_QUANTITY
,NVL (OOL.invoiced_quantity, 0) INVOICED_QUANTITY
,(NVL (OOL.cancelled_quantity, 0) * NVL (OOL.unit_selling_price, 0) ) CANCEL_AMOUNT
,(NVL (OOL.shipped_quantity, 0)   * NVL (OOL.unit_selling_price, 0) ) SHIP_AMOUNT
,(NVL (OOL.invoiced_quantity, 0)  * NVL (OOL.unit_selling_price, 0) ) INVOICED_AMOUNT
,(
CASE
  WHEN OOL.actual_shipment_date <= OOL.request_date
  THEN ( NVL (OOL.shipped_quantity, 0) * NVL (OOL.unit_selling_price, 0) )
  ELSE 0
END ) MET_CRD_AMOUNT
,(
CASE
  WHEN OOL.actual_shipment_date <= OOL.promise_date
  THEN ( NVL (OOL.shipped_quantity, 0) * NVL (OOL.unit_selling_price, 0) )
  ELSE 0
END ) MET_PRD_AMOUNT
,(NVL (OOL.unit_list_price, 0) - NVL (OOL.unit_selling_price, 0) ) * NVL (OOL.ordered_quantity, 0) discount_amount
,NVL(OOL.TAX_VALUE,0) TAX_VALUE --Added new column to fix Jira#497
,nvl(ool.schedule_status_code,'N')  schedule_status_code
FROM 
 (select * from bec_ods.OE_ORDER_LINES_ALL where is_deleted_flg <> 'Y')OOL
,(select * from bec_ods.OE_ORDER_HEADERS_ALL where is_deleted_flg <> 'Y') OOH
,bec_dwh.DIM_CUSTOMER_DETAILS  BILL
,bec_dwh.DIM_CUSTOMER_DETAILS  SHIP
,bec_dwh.DIM_CUSTOMER_DETAILS  LINE_SHIP
,(select * from bec_ods.WSH_DELIVERY_DETAILS where is_deleted_flg <> 'Y') WDD 
,(select * from bec_ods.HZ_CUST_ACCOUNTS where is_deleted_flg <> 'Y') HCA
,(select * from bec_ods.HZ_PARTIES where is_deleted_flg <> 'Y') HP
--,MTL_SYSTEM_ITEMS_B MSI
WHERE 1=1
AND OOH.HEADER_ID  = OOL.HEADER_ID
AND OOH.INVOICE_TO_ORG_ID =BILL.SITE_USE_ID(+)
AND BILL.SITE_USE_CODE(+) = 'BILL_TO'
AND OOH.SHIP_TO_ORG_ID =SHIP.SITE_USE_ID(+)
AND SHIP.SITE_USE_CODE(+) = 'SHIP_TO'
AND OOL.SHIP_TO_ORG_ID =LINE_SHIP.SITE_USE_ID(+)
AND LINE_SHIP.SITE_USE_CODE(+) = 'SHIP_TO'
AND OOH.SOLD_TO_ORG_ID    = HCA.CUST_ACCOUNT_ID(+)
AND HCA.PARTY_ID          = HP.PARTY_ID(+)
AND OOL.HEADER_ID = WDD.SOURCE_HEADER_ID(+) 
AND OOL.LINE_ID  = WDD.SOURCE_LINE_ID (+)  
and 
(OOH.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_order_details' and batch_name = 'om')
OR
OOL.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_order_details' and batch_name = 'om')
)
Group by 
 OOH.INVOICE_TO_ORG_ID
,OOH.SHIP_TO_ORG_ID
,OOH.SOLD_TO_ORG_ID
,OOH.ORG_ID
,OOL.SHIP_FROM_ORG_ID
,OOH.HEADER_ID
,OOL.LINE_ID
,OOL.LINE_NUMBER||'.'||OOL.SHIPMENT_NUMBER
,OOH.ORDER_NUMBER
,OOH.CUST_PO_NUMBER
,hp.party_name
,HCA.ACCOUNT_NUMBER
,BILL.PARTY_NAME 
,BILL.ACCOUNT_NUMBER 
,BILL.LOCATION
,ool.attribute4
,SHIP.PARTY_NAME 
,SHIP.ACCOUNT_NUMBER  
,SHIP.ADDRESS1 
,SHIP.ADDRESS2 
,SHIP.ADDRESS5 
,SHIP.CITY
,SHIP.COUNTY
,UPPER(SHIP.STATE) 
,SHIP.COUNTRY
,OOH.ORDER_TYPE_ID
,OOL.LINE_TYPE_ID
--,OOH.PRICE_LIST_ID
,OOH.ORDER_SOURCE_ID
,OOH.TRANSACTIONAL_CURR_CODE 
,OOH.CONVERSION_RATE
,OOH.FLOW_STATUS_CODE 
--,OOH.ORDERED_DATE
,OOH.PAYMENT_TERM_ID
,OOH.ORIG_SYS_DOCUMENT_REF 
,OOL.FLOW_STATUS_CODE  
,OOL.ORDERED_ITEM
,OOL.ORDERED_QUANTITY
,OOL.SOURCE_TYPE_CODE
,LINE_SHIP.ADDRESS1
,LINE_SHIP.ADDRESS2
,LINE_SHIP.ADDRESS5 
,OOH.CONTEXT
,OOH.ATTRIBUTE1
,OOH.ATTRIBUTE2
,OOH.ATTRIBUTE3
,OOH.ATTRIBUTE4
,OOH.ATTRIBUTE5
,OOH.ATTRIBUTE6
,OOH.ATTRIBUTE7
--,OOH.ATTRIBUTE8
,DECODE(case when OOH.attribute8 = ' India' then 'INDIA' else UPPER(OOH.ATTRIBUTE8) end,'INDIA', '999999', OOH.ATTRIBUTE8)
,OOH.ATTRIBUTE9
,OOH.ATTRIBUTE10
,OOH.ATTRIBUTE11
,OOH.ATTRIBUTE12
,OOH.ATTRIBUTE14
,OOL.ORDERED_QUANTITY 
--,OOL.UNIT_SELLING_PRICE
--,OOL.UNIT_LIST_PRICE
--,(NVL(OOL.ORDERED_QUANTITY,0) * NVL(OOL.UNIT_SELLING_PRICE,0)) 
,(NVL (OOL.ordered_quantity, 0)   * NVL (OOL.unit_selling_price, 0) + NVL(OOL.TAX_VALUE,0) )
,OOL.PRICING_DATE
,OOL.ORDER_QUANTITY_UOM
--,OOL.CREATION_DATE
--,OOL.REQUEST_DATE
--,OOL.SCHEDULE_SHIP_DATE 
,OOL.PRICING_DATE
,OOL.ORDER_QUANTITY_UOM  
,OOL.salesrep_id 
,OOL.line_category_code 
,OOL.booked_flag
,TO_CHAR (OOH.booked_date, 'MON-RRRR') 
,TO_CHAR (OOH.booked_date, 'RRRR') 
,OOL.INVENTORY_ITEM_ID 
,OOL.open_flag 
,OOH.booked_date
,OOH.agreement_id 
,TRUNC (OOH.ordered_date) 
,TRUNC (OOL.request_date) 
,TRUNC (OOL.promise_date) 
,OOL.accounting_rule_id 
,TRUNC (OOL.actual_shipment_date)
,OOL.cancelled_flag
,OOL.created_by
,OOL.last_updated_by
,TRUNC (OOh.creation_date)
,TRUNC (OOL.creation_date) 
,OOL.cust_po_number
,OOL.deliver_to_org_id 
,OOL.fulfilled_flag
,OOL.shippable_flag
,TRUNC (OOL.fulfillment_date) 
,OOL.intmed_ship_to_org_id 
,OOL.invoicing_rule_id
,OOL.item_type_code
,TRUNC (OOL.last_update_date) 
,OOL.price_list_id 
,OOL.return_reason_code
,TRUNC (OOL.schedule_arrival_date) 
,TRUNC (OOL.schedule_ship_date) 
,OOL.ship_to_org_id 
,OOH.sales_channel_code
,NVL (OOL.unit_selling_price, 0) 
,ROUND (NVL (OOL.unit_selling_price, 0), 2)
,NVL (OOL.unit_list_price, 0) 
,ROUND (NVL (OOL.unit_list_price, 0), 2) 
,NVL (OOL.ordered_quantity, 0) 
,NVL (OOL.cancelled_quantity, 0) 
,NVL (OOL.shipped_quantity, 0) 
,NVL (OOL.invoiced_quantity, 0) 
,(NVL (OOL.ordered_quantity, 0)   * NVL (OOL.unit_selling_price, 0) ) 
,(NVL (OOL.cancelled_quantity, 0) * NVL (OOL.unit_selling_price, 0) ) 
,(NVL (OOL.shipped_quantity, 0)   * NVL (OOL.unit_selling_price, 0) ) 
,(NVL (OOL.invoiced_quantity, 0)  * NVL (OOL.unit_selling_price, 0) )
,(
CASE
  WHEN OOL.actual_shipment_date <= OOL.request_date
  THEN ( NVL (OOL.shipped_quantity, 0) * NVL (OOL.unit_selling_price, 0) )
  ELSE 0
END ) 
,(
CASE
  WHEN OOL.actual_shipment_date <= OOL.promise_date
  THEN ( NVL (OOL.shipped_quantity, 0) * NVL (OOL.unit_selling_price, 0) )
  ELSE 0
END ) 
,(NVL (OOL.unit_list_price, 0) - NVL (OOL.unit_selling_price, 0) ) * NVL (OOL.ordered_quantity, 0)
,NVL(OOL.TAX_VALUE,0)
,nvl(ool.schedule_status_code,'N') 
)
);
commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_om_order_details'
	and batch_name = 'om';

commit;