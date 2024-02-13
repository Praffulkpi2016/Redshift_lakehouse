/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_dwh.FACT_AR_TRANSACTION;

CREATE TABLE bec_dwh.FACT_AR_TRANSACTION diststyle all sortkey(INV_TRX_ID,INV_GL_DIST_ID,INV_PAY_SCHEDULE_ID)
AS (
SELECT 
'INVOICE_INFO'      Invoice_Info,
RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID           INV_TRX_ID,
(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID as INV_TRX_ID_KEY,
AR_PAYMENT_SCHEDULES_ALL.LAST_UPDATE_DATE,
RA_CUSTOMER_TRX_ALL.LAST_UPDATED_BY,
RA_CUSTOMER_TRX_ALL.CREATION_DATE,
RA_CUSTOMER_TRX_ALL.CREATED_BY,
RA_CUSTOMER_TRX_ALL.TRX_NUMBER                INV_TRX_NUMBER,
RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID,
RA_CUSTOMER_TRX_ALL.TRX_DATE                  INV_TRX_DATE,
AR_PAYMENT_SCHEDULES_ALL.CLASS				  INV_CLASS, 
RA_CUSTOMER_TRX_ALL.BILL_TO_CONTACT_ID        INV_BILL_TO_CONTACT_ID,
RA_CUSTOMER_TRX_ALL.SOLD_TO_SITE_USE_ID       INV_SOLD_TO_SITE_USE_ID,
(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL.BILL_TO_CUSTOMER_ID       INV_BILL_TO_CUSTOMER_ID_KEY,
RA_CUSTOMER_TRX_ALL.BILL_TO_CUSTOMER_ID       INV_BILL_TO_CUSTOMER_ID,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||(select CUST_ACCT_SITE_ID from (select * from bec_ods.HZ_CUST_SITE_USES_ALL where is_deleted_flg <> 'Y') HZ_CUST_SITE_USES_ALL where site_use_id = RA_CUSTOMER_TRX_ALL.BILL_TO_site_use_id )                 INV_BILL_TO_SITE_ID_KEY,                    
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| RA_CUSTOMER_TRX_ALL.BILL_TO_site_use_id  INV_BILL_TO_SITE_USE_ID_KEY,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| RA_CUSTOMER_TRX_ALL.SHIP_TO_SITE_USE_ID  INV_SHIP_TO_SITE_USE_ID_KEY,
RA_CUSTOMER_TRX_ALL.BILL_TO_SITE_USE_ID       INV_BILL_SITE_USE_ID,
RA_CUSTOMER_TRX_ALL.SHIP_TO_SITE_USE_ID       INV_SHIP_SITE_USE_ID,
RA_CUSTOMER_TRX_ALL.TERM_ID                   INV_TERM_ID,
(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL.TERM_ID as INV_TERM_ID_KEY,
RA_CUSTOMER_TRX_ALL.PRIMARY_SALESREP_ID       INV_SALES_REP_ID,
(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL.PRIMARY_SALESREP_ID as INV_SALES_REP_ID_KEY,
RA_CUSTOMER_TRX_ALL.PURCHASE_ORDER            INV_PO,
RA_CUSTOMER_TRX_ALL.TERRITORY_ID              INV_TERRITORY,
RA_CUSTOMER_TRX_ALL.INVOICE_CURRENCY_CODE     INV_CURRENCY_CODE,
RA_CUSTOMER_TRX_ALL.RECEIPT_METHOD_ID,
RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_ATTRIBUTE1,
RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_CONTEXT,
RA_CUSTOMER_TRX_ALL.PAYING_SITE_USE_ID        INV_PAY_SITE_USE_ID,
AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID  INV_PAY_SCHEDULE_ID,
(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID as INV_PAY_SCHEDULE_ID_KEY,
AR_PAYMENT_SCHEDULES_ALL.DUE_DATE             INV_DUE_DATE,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_ORIGINAL  INV_AMOUNT,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_REMAINING INV_REMAINING_AMOUNT,
AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_SITE_USE_ID INV_CUST_SITE_USE_ID,
AR_PAYMENT_SCHEDULES_ALL.TERMS_SEQUENCE_NUMBER,
AR_PAYMENT_SCHEDULES_ALL.ACTUAL_DATE_CLOSED   INV_ACTUAL_DATE_CLOSED,
AR_PAYMENT_SCHEDULES_ALL.NUMBER_OF_DUE_DATES,
AR_PAYMENT_SCHEDULES_ALL.STATUS               INV_STATUS,
'INV_DIST_INFO'     Invoice_Dist_Info,
RA_CUST_TRX_LINE_GL_DIST_ALL.CUST_TRX_LINE_GL_DIST_ID     INV_GL_DIST_ID,
(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||RA_CUST_TRX_LINE_GL_DIST_ALL.CUST_TRX_LINE_GL_DIST_ID as INV_GL_DIST_ID_KEY,
RA_CUST_TRX_LINE_GL_DIST_ALL.CODE_COMBINATION_ID          INV_CC_ID,
(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||RA_CUST_TRX_LINE_GL_DIST_ALL.CODE_COMBINATION_ID as INV_CC_ID_KEY,
RA_CUST_TRX_LINE_GL_DIST_ALL.SET_OF_BOOKS_ID              INV_LEDGER_ID,
(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||RA_CUST_TRX_LINE_GL_DIST_ALL.SET_OF_BOOKS_ID as INV_LEDGER_ID_KEY,
RA_CUST_TRX_LINE_GL_DIST_ALL.AMOUNT                       INV_DIST_AMOUNT,
RA_CUST_TRX_LINE_GL_DIST_ALL.GL_DATE                      INV_GL_DATE,
RA_CUST_TRX_LINE_GL_DIST_ALL.GL_POSTED_DATE               INV_GL_POST_DATE,
RA_CUST_TRX_LINE_GL_DIST_ALL.ACCTD_AMOUNT                 INV_ACCT_AMOUNT,
RA_CUST_TRX_TYPES_ALL.CUST_TRX_TYPE_ID                    INV_TRX_TYPE_ID,
RA_CUSTOMER_TRX_ALL.ORG_ID                                INV_ORG_ID,
(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL.ORG_ID as INV_ORG_ID_KEY,
RA_CUSTOMER_TRX_ALL.ORG_ID as ORG_ID,
RA_CUSTOMER_TRX_ALL.LEGAL_ENTITY_ID                       INV_LEGAL_ENTITY_ID,
(select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL.LEGAL_ENTITY_ID as INV_LEGAL_ENTITY_ID_KEY,
RA_CUST_TRX_LINE_GL_DIST_ALL.ACCOUNT_CLASS                INV_DIST_ACCT_CLASS_CODE,
RA_CUST_TRX_LINE_GL_DIST_ALL.LATEST_REC_FLAG              INV_DIST_LATEST_REC_FLAG,
--AR_PAYMENT_SCHEDULES_ALL.LAST_UPDATE_DATE,
--RA_CUST_TRX_LINE_GL_DIST_ALL.LAST_UPDATE_DATE,
RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_ATTRIBUTE1        AS PROJ_NUM,
RA_BATCH_SOURCES_ALL.BATCH_SOURCE_TYPE, 
RA_CUSTOMER_TRX_LINES_ALL.INVENTORY_ITEM_ID , 
RA_CUSTOMER_TRX_LINES_ALL.WAREHOUSE_ID as ORGANIZATION_ID
,RA_CUSTOMER_TRX_ALL.INVOICE_CURRENCY_CODE
,RA_CUSTOMER_TRX_ALL.INVOICING_RULE_ID
,RA_CUSTOMER_TRX_ALL.CUSTOMER_REFERENCE
,RA_CUSTOMER_TRX_ALL.STATUS_TRX     as Receipt_Status
,RA_CUSTOMER_TRX_LINES_ALL.QUANTITY_ORDERED
,RA_CUSTOMER_TRX_LINES_ALL.UNIT_SELLING_PRICE
,RA_CUSTOMER_TRX_LINES_ALL.UOM_CODE
,RA_CUSTOMER_TRX_LINES_ALL.REVENUE_AMOUNT,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| 
RA_CUSTOMER_TRX_LINES_ALL.INVENTORY_ITEM_ID   INVENTORY_ITEM_ID_KEY,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_APPLIED,
cast(NVL(AR_PAYMENT_SCHEDULES_ALL.AMOUNT_APPLIED,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT_APPLIED,
cast(NVL(AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_ORIGINAL,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_INVOICE_AMOUNT,
cast(NVL(RA_CUSTOMER_TRX_LINES_ALL.REVENUE_AMOUNT,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_LINE_AMOUNT,
cast(NVL(RA_CUST_TRX_LINE_GL_DIST_ALL.AMOUNT ,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_DIST_AMOUNT,
ar_cash_Receipts_all.EXCHANGE_RATE_TYPE,
ar_cash_Receipts_all.EXCHANGE_RATE,
ar_cash_Receipts_all.EXCHANGE_DATE as EXCHANGE_RATE_DATE,
RA_RULES.name as ACCOUNTING_RULE,
RA_CUSTOMER_TRX_LINES_ALL.quantity_invoiced,
RA_CUSTOMER_TRX_LINES_ALL.line_number,
'N' AS IS_DELETED_FLG,
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
    || '-'
       || nvl(RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID, 0)
       ||'-'||nvl(RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID,0)
	   || '-'
       || nvl(RA_CUST_TRX_LINE_GL_DIST_ALL.CUST_TRX_LINE_GL_DIST_ID, 0)
	   || '-'
       || nvl(AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID, 0) as dw_load_id, 
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from (select * from bec_ods.RA_CUSTOMER_TRX_ALL where is_deleted_flg <> 'Y') RA_CUSTOMER_TRX_ALL  
inner join (select * from bec_ods.RA_CUSTOMER_TRX_LINES_ALL where is_deleted_flg <> 'Y') RA_CUSTOMER_TRX_LINES_ALL
on  RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID  
inner JOIN (select * from bec_ods.RA_BATCH_SOURCES_ALL where is_deleted_flg <> 'Y') RA_BATCH_SOURCES_ALL on 
RA_CUSTOMER_TRX_ALL.BATCH_SOURCE_ID = RA_BATCH_SOURCES_ALL.BATCH_SOURCE_ID
and RA_CUSTOMER_TRX_ALL.org_id = RA_BATCH_SOURCES_ALL.org_id
inner join (select * from bec_ods.AR_PAYMENT_SCHEDULES_ALL where is_deleted_flg <> 'Y') AR_PAYMENT_SCHEDULES_ALL
on RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_TRX_ID
INNER JOIN (select * from bec_ods.RA_CUST_TRX_LINE_GL_DIST_ALL where is_deleted_flg <> 'Y') RA_CUST_TRX_LINE_GL_DIST_ALL
ON RA_CUST_TRX_LINE_GL_DIST_ALL.CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID
and RA_CUST_TRX_LINE_GL_DIST_ALL.CUSTOMER_TRX_LINE_ID = RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID
LEFT OUTER JOIN (select * from bec_ods.RA_CUST_TRX_TYPES_ALL where is_deleted_flg <> 'Y') RA_CUST_TRX_TYPES_ALL
ON RA_CUST_TRX_TYPES_ALL.CUST_TRX_TYPE_ID = RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID 
AND RA_CUST_TRX_TYPES_ALL.ORG_ID = RA_CUSTOMER_TRX_ALL.ORG_ID
left outer join (select * from bec_ods.AR_CASH_RECEIPTS_ALL where is_deleted_flg <> 'Y') AR_CASH_RECEIPTS_ALL on 
AR_PAYMENT_SCHEDULES_ALL.CASH_RECEIPT_ID = AR_CASH_RECEIPTS_ALL.CASH_RECEIPT_ID
left outer join 
( select * from (select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y') GL_DAILY_RATES 
where conversion_type = 'Corporate' and to_currency = 'USD')DCR
on  DCR.from_currency = AR_PAYMENT_SCHEDULES_ALL.invoice_currency_code
and DCR.conversion_date= AR_PAYMENT_SCHEDULES_ALL.GL_DATE
left outer join (select * from bec_ods.RA_RULES where is_deleted_flg <> 'Y') RA_RULES on ra_rules.RULE_ID =	RA_CUSTOMER_TRX_LINES_ALL.ACCOUNTING_RULE_ID
WHERE 1=1   
AND RA_CUSTOMER_TRX_ALL.COMPLETE_FLAG = 'Y' 
--AND RA_CUST_TRX_LINE_GL_DIST_ALL.ACCOUNT_CLASS IN('REC') 
--AND RA_CUST_TRX_LINE_GL_DIST_ALL.LATEST_REC_FLAG = 'Y' 
AND AR_PAYMENT_SCHEDULES_ALL.CLASS <> 'PMT' 
AND AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID <> -1
)
;

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ar_transaction'
	and batch_name = 'ar';

commit;