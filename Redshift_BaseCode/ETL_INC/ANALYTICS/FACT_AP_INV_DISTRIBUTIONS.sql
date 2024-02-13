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
	bec_dwh.FACT_AP_INV_DISTRIBUTIONS
where
	(nvl(INVOICE_ID,0) ,
	nvl(INVOICE_LINE_NUMBER,0),
	nvl(INVOICE_DISTRIBUTION_ID,0)) in (
	select
		nvl(ods.INVOICE_ID,0) as INVOICE_ID,
		nvl(ods.INVOICE_LINE_NUMBER,0) as INVOICE_LINE_NUMBER,
		nvl(ods.INVOICE_DISTRIBUTION_ID,0) as INVOICE_DISTRIBUTION_ID
	from
		bec_dwh.fact_ap_inv_distributions dw,
		(
		select
			AID.INVOICE_ID,
			AID.INVOICE_LINE_NUMBER,
			AID.INVOICE_DISTRIBUTION_ID
from 
bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AID
inner join 
bec_ods.AP_INVOICES_ALL AIA on AID.INVOICE_ID = AIA.INVOICE_ID
left outer join 
bec_ods.AP_INVOICE_LINES_ALL AILA on AIA.INVOICE_ID = AILA.INVOICE_ID
and AID.INVOICE_LINE_NUMBER=AILA.LINE_NUMBER
						where 1=1 and (AID.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_inv_distributions'
				and batch_name = 'ap'))) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' ||
nvl(ods.INVOICE_ID, 0)
|| '-' || nvl(ods.INVOICE_LINE_NUMBER, 0)|| '-' || nvl(ods.INVOICE_DISTRIBUTION_ID, 0)
);

commit;
-- Insert records

insert
	into
	bec_dwh.FACT_AP_INV_DISTRIBUTIONS 
(
 ACCOUNTING_DATE,
	ASSETS_ADDITION_FLAG,
	ASSETS_TRACKING_FLAG,
	DISTRIBUTION_LINE_NUMBER,
	GL_ACCOUNT_ID_KEY,
	INVOICE_ID_KEY,
	INVOICE_ID,
	LINE_TYPE_LOOKUP_CODE,
	PERIOD_NAME,
	LEDGER_ID_KEY,
	VENDOR_ID_KEY,
	VENDOR_SITE_ID_KEY,
	TERMS_ID_KEY,
	BATCH_ID_KEY,
	AMOUNT,
	BASE_AMOUNT,
	DESCRIPTION,
	FINAL_MATCH_FLAG,
	POSTED_FLAG,
	PO_DISTRIBUTION_ID_KEY,
	QUANTITY_INVOICED,
	REVERSAL_FLAG,
	UNIT_PRICE,
	EXPENDITURE_ITEM_DATE,
	EXPENDITURE_ORGANIZATION_ID,
	EXPENDITURE_TYPE,
	PARENT_INVOICE_ID,
	PREPAY_AMOUNT_REMAINING,
	PROJECT_ID_KEY,
	PROJECT_TASK_ID_KEY,	
	ORG_ID_KEY,
	ORG_ID,
	RECEIPT_VERIFIED_FLAG,
	RECEIPT_REQUIRED_FLAG,
	RECEIPT_MISSING_FLAG,
	JUSTIFICATION,
	EXPENSE_GROUP,
	START_EXPENSE_DATE,
	END_EXPENSE_DATE,
	RECEIPT_CURRENCY_CODE,
	RECEIPT_CONVERSION_RATE,
	RECEIPT_CURRENCY_AMOUNT,
	DAILY_AMOUNT,
	ADJUSTMENT_REASON,
	RCV_TRANSACTION_ID,
	INVOICE_DISTRIBUTION_ID,
	MERCHANT_DOCUMENT_NUMBER,
	MERCHANT_NAME,
	MERCHANT_REFERENCE,
	MERCHANT_TAX_REG_NUMBER,
	MERCHANT_TAXPAYER_ID,
	COUNTRY_OF_SUPPLY,
	ACCOUNTING_EVENT_ID,
	CANCELLATION_FLAG,
	INVOICE_LINE_NUMBER,
	ASSET_BOOK_TYPE_CODE,
	ASSET_CATEGORY_ID,
	SUMMARY_TAX_LINE_ID,
	TAX_CODE_ID,
	TOTAL_DIST_AMOUNT,
	TOTAL_DIST_BASE_AMOUNT,
	CANCELLED_FLAG,
	PO_HEADER_ID,
	PO_LINE_ID,
	GBL_DIST_AMOUNT,
	IS_DELETED_FLG,
	SOURCE_APP_ID,
	DW_LOAD_ID,
	DW_INSERT_DATE,
	DW_UPDATE_DATE)
(
select
	AID.ACCOUNTING_DATE,
	AID.ASSETS_ADDITION_FLAG,
	AID.ASSETS_TRACKING_FLAG,
	AID.DISTRIBUTION_LINE_NUMBER,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AID.DIST_CODE_COMBINATION_ID as GL_ACCOUNT_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AID.invoice_id as INVOICE_ID_KEY,
	AID.INVOICE_ID,
	AID.LINE_TYPE_LOOKUP_CODE,
	AID.PERIOD_NAME,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AID.SET_OF_BOOKS_ID as LEDGER_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.VENDOR_ID as VENDOR_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.TERMS_ID as TERMS_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || nvl(AIA.BATCH_ID, 0) as BATCH_ID_KEY,
	AID.AMOUNT,
	AID.BASE_AMOUNT,
	AID.DESCRIPTION,
	AID.FINAL_MATCH_FLAG,
	AID.POSTED_FLAG,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AID.PO_DISTRIBUTION_ID as PO_DISTRIBUTION_ID_KEY,
	AID.QUANTITY_INVOICED,
	AID.REVERSAL_FLAG,
	AID.UNIT_PRICE,
	AID.EXPENDITURE_ITEM_DATE,
	AID.EXPENDITURE_ORGANIZATION_ID,
	AID.EXPENDITURE_TYPE,
	AID.PARENT_INVOICE_ID,
	AID.PREPAY_AMOUNT_REMAINING,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AID.PROJECT_ID as PROJECT_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AID.PROJECT_ID || '-' || AID.TASK_ID as PROJECT_TASK_ID_KEY,	
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AID.ORG_ID as ORG_ID_KEY,
	AID.ORG_ID as ORG_ID,
	AID.RECEIPT_VERIFIED_FLAG,
	AID.RECEIPT_REQUIRED_FLAG,
	AID.RECEIPT_MISSING_FLAG,
	AID.JUSTIFICATION,
	AID.EXPENSE_GROUP,
	AID.START_EXPENSE_DATE,
	AID.END_EXPENSE_DATE,
	AID.RECEIPT_CURRENCY_CODE,
	AID.RECEIPT_CONVERSION_RATE,
	AID.RECEIPT_CURRENCY_AMOUNT,
	AID.DAILY_AMOUNT,
	AID.ADJUSTMENT_REASON,
	AID.RCV_TRANSACTION_ID,
	AID.INVOICE_DISTRIBUTION_ID,
	AID.MERCHANT_DOCUMENT_NUMBER,
	AID.MERCHANT_NAME,
	AID.MERCHANT_REFERENCE,
	AID.MERCHANT_TAX_REG_NUMBER,
	AID.MERCHANT_TAXPAYER_ID,
	AID.COUNTRY_OF_SUPPLY,
	AID.ACCOUNTING_EVENT_ID,
	AID.CANCELLATION_FLAG,
	AID.INVOICE_LINE_NUMBER,
	AID.ASSET_BOOK_TYPE_CODE,
	AID.ASSET_CATEGORY_ID,
	AID.SUMMARY_TAX_LINE_ID,
	AID.TAX_CODE_ID,
	AID.TOTAL_DIST_AMOUNT,
	AID.TOTAL_DIST_BASE_AMOUNT,
	AID.CANCELLED_FLAG,
	AILA.PO_HEADER_ID,
	AILA.PO_LINE_ID,
	cast(NVL(AID.TOTAL_DIST_AMOUNT, 0) * NVL(DCR.conversion_rate, 1) as decimal(18, 2)) GBL_DIST_AMOUNT,
	'N' as IS_DELETED_FLG,
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
       || nvl(AID.INVOICE_ID, 0) || '-' || nvl(AID.INVOICE_LINE_NUMBER, 0)|| '-' || nvl(AID.INVOICE_DISTRIBUTION_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(select * from bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y') AID
inner join 
(select * from bec_ods.AP_INVOICES_ALL where is_deleted_flg <> 'Y') AIA on AID.INVOICE_ID = AIA.INVOICE_ID
left outer join 
(select * from bec_ods.AP_INVOICE_LINES_ALL where is_deleted_flg <> 'Y') AILA on AIA.INVOICE_ID = AILA.INVOICE_ID
and AID.INVOICE_LINE_NUMBER=AILA.LINE_NUMBER
left outer join (select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y' and to_currency = 'USD' and conversion_type = 'Corporate') DCR
on  AIA.invoice_currency_code = DCR.from_currency
and AIA.invoice_date = DCR.conversion_date
where 1=1
	and (AID.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_inv_distributions'
				and batch_name = 'ap')
		)
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ap_inv_distributions'
	and batch_name = 'ap';

commit;