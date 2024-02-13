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
	-- Delete Records
	delete
	from
	bec_dwh.FACT_AR_TRANSACTION
	where
	(nvl(INV_TRX_ID,0),nvl(CUSTOMER_TRX_LINE_ID,0),nvl(INV_GL_DIST_ID,0),nvl(INV_PAY_SCHEDULE_ID,0)) 
	in
	(
		SELECT
		nvl(TMP.INV_TRX_ID, 0) as INV_TRX_ID
		,nvl(TMP.CUSTOMER_TRX_LINE_ID,0) as CUSTOMER_TRX_LINE_ID
		,nvl(TMP.INV_GL_DIST_ID, 0) as INV_GL_DIST_ID
		,nvl(TMP.INV_PAY_SCHEDULE_ID, 0) as INV_PAY_SCHEDULE_ID
		from bec_dwh.FACT_AR_TRANSACTION dw,
		(
			SELECT 
			RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID           INV_TRX_ID,
			RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID,
			AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID  INV_PAY_SCHEDULE_ID,
			RA_CUST_TRX_LINE_GL_DIST_ALL.CUST_TRX_LINE_GL_DIST_ID     INV_GL_DIST_ID 
			from 
			bec_ods.RA_CUSTOMER_TRX_ALL RA_CUSTOMER_TRX_ALL  
			inner join bec_ods.RA_CUSTOMER_TRX_LINES_ALL RA_CUSTOMER_TRX_LINES_ALL
			on  RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID  
			inner JOIN bec_ods.RA_BATCH_SOURCES_ALL RA_BATCH_SOURCES_ALL 
			on RA_CUSTOMER_TRX_ALL.BATCH_SOURCE_ID = RA_BATCH_SOURCES_ALL.BATCH_SOURCE_ID
			and RA_CUSTOMER_TRX_ALL.org_id = RA_BATCH_SOURCES_ALL.org_id
			inner join bec_ods.AR_PAYMENT_SCHEDULES_ALL AR_PAYMENT_SCHEDULES_ALL
			on RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_TRX_ID
			INNER JOIN bec_ods.RA_CUST_TRX_LINE_GL_DIST_ALL RA_CUST_TRX_LINE_GL_DIST_ALL
			ON RA_CUST_TRX_LINE_GL_DIST_ALL.CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID
			and RA_CUST_TRX_LINE_GL_DIST_ALL.CUSTOMER_TRX_LINE_ID = RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID
			LEFT OUTER JOIN bec_ods.RA_CUST_TRX_TYPES_ALL RA_CUST_TRX_TYPES_ALL
			ON RA_CUST_TRX_TYPES_ALL.CUST_TRX_TYPE_ID = RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID 
			AND RA_CUST_TRX_TYPES_ALL.ORG_ID = RA_CUSTOMER_TRX_ALL.ORG_ID
			left outer join bec_ods.AR_CASH_RECEIPTS_ALL AR_CASH_RECEIPTS_ALL 
			on AR_PAYMENT_SCHEDULES_ALL.CASH_RECEIPT_ID = AR_CASH_RECEIPTS_ALL.CASH_RECEIPT_ID
			left outer join 
			( select * from bec_ods.GL_DAILY_RATES GL_DAILY_RATES 
			where conversion_type = 'Corporate' and to_currency(+) = 'USD')DCR
			on  DCR.from_currency = AR_PAYMENT_SCHEDULES_ALL.invoice_currency_code
			and DCR.conversion_date= AR_PAYMENT_SCHEDULES_ALL.GL_DATE
			left outer join bec_ods.RA_RULES RA_RULES 
			on ra_rules.RULE_ID =	RA_CUSTOMER_TRX_LINES_ALL.ACCOUNTING_RULE_ID
			WHERE 1=1   
			AND RA_CUSTOMER_TRX_ALL.COMPLETE_FLAG = 'Y'   
			AND AR_PAYMENT_SCHEDULES_ALL.CLASS <> 'PMT' 
			AND AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID <> -1
			AND ( RA_CUST_TRX_LINE_GL_DIST_ALL.kca_seq_date > 
				(select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
				where dw_table_name ='fact_ar_transaction' and batch_name = 'ar')
				OR
				AR_PAYMENT_SCHEDULES_ALL.kca_seq_date > 
				(select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
				where dw_table_name ='fact_ar_transaction' and batch_name = 'ar')
				or RA_CUSTOMER_TRX_ALL.is_deleted_flg = 'Y'
				or RA_CUSTOMER_TRX_LINES_ALL.is_deleted_flg = 'Y'
				or RA_BATCH_SOURCES_ALL.is_deleted_flg = 'Y'
				or AR_PAYMENT_SCHEDULES_ALL.is_deleted_flg = 'Y'
				or RA_CUST_TRX_LINE_GL_DIST_ALL.is_deleted_flg = 'Y'
				or RA_CUST_TRX_TYPES_ALL.is_deleted_flg = 'Y'
				or AR_CASH_RECEIPTS_ALL.is_deleted_flg = 'Y'
				or DCR.is_deleted_flg = 'Y'
			or RA_RULES.is_deleted_flg = 'Y')
		) TMP
		WHERE 1=1
		AND DW.DW_LOAD_ID
		= 
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
			source_system = 'EBS'
		)
		|| '-'
		|| nvl(TMP.INV_TRX_ID, 0)
		||'-'||nvl(TMP.CUSTOMER_TRX_LINE_ID,0)
		|| '-'
		|| nvl(TMP.INV_GL_DIST_ID, 0)
		|| '-'
		|| nvl(TMP.INV_PAY_SCHEDULE_ID, 0)
	);
	commit;
	-- Insert records
	
	insert
	into
	bec_dwh.FACT_AR_TRANSACTION 
	(
		invoice_info,
		inv_trx_id,
		inv_trx_id_key,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		inv_trx_number,
		customer_trx_line_id,
		inv_trx_date,
		inv_class,
		inv_bill_to_contact_id,
		inv_sold_to_site_use_id,
		inv_bill_to_customer_id_key,
		inv_bill_to_customer_id,
		inv_bill_to_site_id_key,
		inv_bill_to_site_use_id_key,
		inv_ship_to_site_use_id_key,
		inv_bill_site_use_id,
		inv_ship_site_use_id,
		inv_term_id,
		inv_term_id_key,
		inv_sales_rep_id,
		inv_sales_rep_id_key,
		inv_po,
		inv_territory,
		inv_currency_code,
		receipt_method_id,
		interface_header_attribute1,
		interface_header_context,
		inv_pay_site_use_id,
		inv_pay_schedule_id,
		inv_pay_schedule_id_key,
		inv_due_date,
		inv_amount,
		inv_remaining_amount,
		inv_cust_site_use_id,
		terms_sequence_number,
		inv_actual_date_closed,
		number_of_due_dates,
		inv_status,
		invoice_dist_info,
		inv_gl_dist_id,
		inv_gl_dist_id_key,
		inv_cc_id,
		inv_cc_id_key,
		inv_ledger_id,
		inv_ledger_id_key,
		inv_dist_amount,
		inv_gl_date,
		inv_gl_post_date,
		inv_acct_amount,
		inv_trx_type_id,
		inv_org_id,
		inv_org_id_key,
		ORG_ID,
		inv_legal_entity_id,
		inv_legal_entity_id_key,
		inv_dist_acct_class_code,
		inv_dist_latest_rec_flag,
		proj_num,
		batch_source_type,
		inventory_item_id,
		organization_id,
		invoice_currency_code,
		invoicing_rule_id,
		customer_reference,
		receipt_status,
		quantity_ordered,
		unit_selling_price,
		uom_code,
		revenue_amount,
		inventory_item_id_key,
		amount_applied,
		GBL_AMOUNT_APPLIED,
		GBL_INVOICE_AMOUNT,
		GBL_LINE_AMOUNT,
		GBL_DIST_AMOUNT,	
		exchange_rate_type,
		exchange_rate,
		exchange_rate_date,
		accounting_rule,
		quantity_invoiced,
		line_number,
		IS_DELETED_FLG,
		source_app_id,
		dw_load_id,
		dw_insert_date,
		dw_update_date
	)
	(
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
		where conversion_type = 'Corporate' and to_currency(+) = 'USD')DCR
		on  DCR.from_currency = AR_PAYMENT_SCHEDULES_ALL.invoice_currency_code
		and DCR.conversion_date= AR_PAYMENT_SCHEDULES_ALL.GL_DATE
		left outer join (select * from bec_ods.RA_RULES where is_deleted_flg <> 'Y') RA_RULES on ra_rules.RULE_ID =	RA_CUSTOMER_TRX_LINES_ALL.ACCOUNTING_RULE_ID
		WHERE 1=1   
		AND RA_CUSTOMER_TRX_ALL.COMPLETE_FLAG = 'Y' 
		--AND RA_CUST_TRX_LINE_GL_DIST_ALL.ACCOUNT_CLASS IN('REC') 
		--AND RA_CUST_TRX_LINE_GL_DIST_ALL.LATEST_REC_FLAG = 'Y' 
		AND AR_PAYMENT_SCHEDULES_ALL.CLASS <> 'PMT' 
		AND AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID <> -1
		
		AND ( RA_CUST_TRX_LINE_GL_DIST_ALL.kca_seq_date > 
			(select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
			where dw_table_name ='fact_ar_transaction' and batch_name = 'ar')
			OR
			AR_PAYMENT_SCHEDULES_ALL.kca_seq_date > 
			(select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
			where dw_table_name ='fact_ar_transaction' and batch_name = 'ar')
		)
	);
	COMMIT;
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
last_refresh_date = getdate()
WHERE
dw_table_name  = 'fact_ar_transaction'
and batch_name = 'ar';

COMMIT;