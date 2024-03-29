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
	bec_dwh.fact_ap_invoice_details
where
	(nvl(INVOICE_ID,0),
	nvl(LINE_NUMBER,0)) in (
	select
		nvl(ods.INVOICE_ID,0) as INVOICE_ID,
		nvl(ods.LINE_NUMBER,0) as LINE_NUMBER
	from
		bec_dwh.fact_ap_invoice_details dw,
		(
		select
			AILA.INVOICE_ID,
			AILA.LINE_NUMBER as LINE_NUMBER,
			AIA.LAST_UPDATE_DATE,
			AILA.KCA_OPERATION,
			AIA.kca_seq_date,
			AILA.is_deleted_flg,
			AIA.is_deleted_flg is_deleted_flg1
		from
			bec_ods.AP_INVOICE_LINES_ALL AILA,
			bec_ods.AP_INVOICES_ALL AIA
		where
			AIA.INVOICE_ID = AILA.INVOICE_ID) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.INVOICE_ID, 0) || '-' || nvl(ods.LINE_NUMBER, 0)
			and (ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_invoice_details'
				and batch_name = 'ap')
				or ods.is_deleted_flg = 'Y'
				or ods.is_deleted_flg1 = 'Y')
);

commit;
-- Insert records

insert
	into
	bec_dwh.fact_ap_invoice_details
( 
  INVOICE_ID_KEY,
	VENDOR_ID_KEY,
	INVOICE_NUM,
	LEDGER_ID_KEY,
	INVOICE_CURRENCY_CODE,
	PAYMENT_CURRENCY_CODE,
	INVOICE_AMOUNT,
	VENDOR_SITE_ID_KEY,
	AMOUNT_PAID,
	DISCOUNT_AMOUNT_TAKEN,
	INVOICE_DATE,
	source,
	INVOICE_TYPE_LOOKUP_CODE,
	DESCRIPTION,
	TAX_AMOUNT,
	TERMS_ID_KEY,
	TERMS_DATE,
	PAYMENT_STATUS_FLAG,
	PO_HEADER_ID,
	FREIGHT_AMOUNT,
	INVOICE_ID,
	INVOICE_RECEIVED_DATE,
	EXCHANGE_RATE,
	EXCHANGE_RATE_TYPE,
	EXCHANGE_DATE,
	CANCELLED_DATE,
	CREATION_DATE,
	LAST_UPDATE_DATE,
	CANCELLED_AMOUNT,
	PROJECT_ID_KEY,
	PROJECT_TASK_ID_KEY,
	ORG_ID_KEY,
	 ORG_ID,
	PAY_CURR_INVOICE_AMOUNT,
	GL_DATE,
	TOTAL_TAX_AMOUNT,
	LEGAL_ENTITY_ID,
	PARTY_ID,
	PARTY_SITE_ID,
	REMIT_TO_SUPPLIER_NAME,
	REMIT_TO_SUPPLIER_SITE,
	AMOUNT_APPLICABLE_TO_DISCOUNT,
	PAYMENT_METHOD_LOOKUP_CODE,
	PAY_GROUP_LOOKUP_CODE,
	ACCTS_PAY_CODE_COMBINATION_ID,
	INV_AMT_FUNC_CURRENCY,
	ORIGINAL_PREPAYMENT_AMOUNT,
	PREPAY_FLAG,
	EXPENDITURE_TYPE,
	PAYMENT_AMOUNT_TOTAL,
	LINE_NUMBER,
	LINE_TYPE_LOOKUP_CODE,
	LINE_DESCRIPTION,
	LINE_SOURCE,
	LINE_ORG_ID,
	INVENTORY_ITEM_ID_KEY,
	ITEM_DESCRIPTION,
	MATCH_TYPE,
	DEFAULT_DIST_CCID,
	ACCOUNTING_DATE,
	PERIOD_NAME,
	AMOUNT,
	INVLIN_AMT_FUNC_CURRENCY,
	ROUNDING_AMT,
	QUANTITY_INVOICED,
	UNIT_MEAS_LOOKUP_CODE,
	UNIT_PRICE,
	CANCELLED_FLAG,
	INCOME_TAX_REGION,
	PREPAY_INVOICE_ID,
	PREPAY_LINE_NUMBER,
	LINE_PO_HEADER,
	PO_LINE_ID,
	PO_RELEASE_ID,
	PO_LINE_LOCATION_ID,
	PO_DISTRIBUTION_ID,
	RCV_TRANSACTION_ID,
	ASSETS_TRACKING_FLAG,
	ASSET_BOOK_TYPE_CODE,
	ASSET_CATEGORY_ID,
	LINE_EXPENDITURE_TYPE,
	TAX_REGIME_CODE,
	TAX,
	TAX_JURISDICTION_CODE,
	TAX_STATUS_CODE,
	TAX_RATE_ID,
	TAX_RATE_CODE,
	TAX_RATE,
	TAX_CODE_ID,
	TAX_CLASSIFICATION_CODE,
	RCV_SHIPMENT_LINE_ID,
	WFAPPROVAL_STATUS,
	GBL_INVOICE_AMOUNT,
	GBL_AMOUNT_PAID,
	IS_DELETED_FLG,
	SOURCE_APP_ID,
	DW_LOAD_ID,
	DW_INSERT_DATE,
	DW_UPDATE_DATE)
(select
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.INVOICE_ID as INVOICE_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.VENDOR_ID as VENDOR_ID_KEY,
	AIA.INVOICE_NUM,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.SET_OF_BOOKS_ID as LEDGER_ID_KEY,
	AIA.INVOICE_CURRENCY_CODE,
	AIA.PAYMENT_CURRENCY_CODE,
	AIA.INVOICE_AMOUNT,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
	AIA.AMOUNT_PAID,
	AIA.DISCOUNT_AMOUNT_TAKEN,
	AIA.INVOICE_DATE,
	AIA.SOURCE,
	AIA.INVOICE_TYPE_LOOKUP_CODE,
	AIA.DESCRIPTION,
	AIA.TAX_AMOUNT,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.TERMS_ID as TERMS_ID_KEY,	
	AIA.TERMS_DATE,
	AIA.PAYMENT_STATUS_FLAG,
	AIA.PO_HEADER_ID,
	AIA.FREIGHT_AMOUNT,
	AIA.INVOICE_ID,
	AIA.INVOICE_RECEIVED_DATE,
	AIA.EXCHANGE_RATE,
	AIA.EXCHANGE_RATE_TYPE,
	AIA.EXCHANGE_DATE,
	AIA.CANCELLED_DATE,
	AIA.CREATION_DATE,
	AIA.LAST_UPDATE_DATE,
	AIA.CANCELLED_AMOUNT,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.PROJECT_ID as PROJECT_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.PROJECT_ID || '-' || AIA.TASK_ID as PROJECT_TASK_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AIA.ORG_ID as ORG_ID_KEY,
		AIA.ORG_ID as ORG_ID,
	AIA.PAY_CURR_INVOICE_AMOUNT,
	AIA.GL_DATE,
	AIA.TOTAL_TAX_AMOUNT,
	AIA.LEGAL_ENTITY_ID,
	AIA.PARTY_ID,
	AIA.PARTY_SITE_ID,
	AIA.REMIT_TO_SUPPLIER_NAME,
	AIA.REMIT_TO_SUPPLIER_SITE,
	AIA.AMOUNT_APPLICABLE_TO_DISCOUNT,
	AIA.PAYMENT_METHOD_LOOKUP_CODE,
	AIA.PAY_GROUP_LOOKUP_CODE,
	AIA.ACCTS_PAY_CODE_COMBINATION_ID,
	AIA.BASE_AMOUNT "INV_AMT_FUNC_CURRENCY",
	AIA.ORIGINAL_PREPAYMENT_AMOUNT,
	AIA.PREPAY_FLAG,
	AIA.EXPENDITURE_TYPE,
	AIA.PAYMENT_AMOUNT_TOTAL,
	AILA.LINE_NUMBER,
	AILA.LINE_TYPE_LOOKUP_CODE,
	AILA.DESCRIPTION "LINE_DESCRIPTION",
	AILA.LINE_SOURCE,
	AILA.ORG_ID "LINE_ORG_ID",
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || AILA.INVENTORY_ITEM_ID as INVENTORY_ITEM_ID_KEY,
	AILA.ITEM_DESCRIPTION,
	AILA.MATCH_TYPE,
	AILA.DEFAULT_DIST_CCID,
	AILA.ACCOUNTING_DATE,
	AILA.PERIOD_NAME,
	AILA.AMOUNT,
	AILA.BASE_AMOUNT "INVLIN_AMT_FUNC_CURRENCY",
	AILA.ROUNDING_AMT,
	AILA.QUANTITY_INVOICED,
	AILA.UNIT_MEAS_LOOKUP_CODE,
	AILA.UNIT_PRICE,
	AILA.CANCELLED_FLAG,
	AILA.INCOME_TAX_REGION,
	AILA.PREPAY_INVOICE_ID,
	AILA.PREPAY_LINE_NUMBER,
	AILA.PO_HEADER_ID LINE_PO_HEADER,
	AILA.PO_LINE_ID,
	AILA.PO_RELEASE_ID,
	AILA.PO_LINE_LOCATION_ID,
	AILA.PO_DISTRIBUTION_ID,
	AILA.RCV_TRANSACTION_ID,
	AILA.ASSETS_TRACKING_FLAG,
	AILA.ASSET_BOOK_TYPE_CODE,
	AILA.ASSET_CATEGORY_ID,	
	AILA.EXPENDITURE_TYPE LINE_EXPENDITURE_TYPE,
	AILA.TAX_REGIME_CODE,
	AILA.TAX,
	AILA.TAX_JURISDICTION_CODE,
	AILA.TAX_STATUS_CODE,
	AILA.TAX_RATE_ID,
	AILA.TAX_RATE_CODE,
	AILA.TAX_RATE,
	AILA.TAX_CODE_ID,
	AILA.TAX_CLASSIFICATION_CODE,
	AILA.RCV_SHIPMENT_LINE_ID,
	AILA.WFAPPROVAL_STATUS,
	cast(NVL(AIA.invoice_amount, 0) * NVL(DCR.conversion_rate, 1) as DECIMAL(18, 2)) GBL_INVOICE_AMOUNT,
	cast(NVL(AIA.amount_paid, 0) * NVL(DCR.conversion_rate, 1) as DECIMAL(18, 2)) GBL_AMOUNT_PAID,
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
       || nvl(AILA.INVOICE_ID, 0) || '-'
       || nvl(AILA.LINE_NUMBER, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(select * from bec_ods.AP_INVOICE_LINES_ALL where is_deleted_flg <> 'Y') AILA,
	(select * from bec_ods.AP_INVOICES_ALL where is_deleted_flg <> 'Y') AIA,
    (select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y') DCR
where
	1 = 1
	and AIA.INVOICE_ID = AILA.INVOICE_ID
	and DCR.to_currency(+) = 'USD'
	and DCR.conversion_type(+) = 'Corporate'
	and AIA.invoice_currency_code = DCR.from_currency(+)
	and DCR.conversion_date(+) = AIA.invoice_date
	and (AIA.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_invoice_details'
				and batch_name = 'ap'))
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ap_invoice_details'
	and batch_name = 'ap';

commit;