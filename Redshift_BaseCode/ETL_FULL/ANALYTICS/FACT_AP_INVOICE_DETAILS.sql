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

drop table if exists bec_dwh.fact_ap_invoice_details;

create table bec_dwh.fact_ap_invoice_details
diststyle all
	sortkey (INVOICE_ID,
LINE_NUMBER)
as
(
select
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIA.INVOICE_ID as INVOICE_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIA.VENDOR_ID as VENDOR_ID_KEY,
	AIA.INVOICE_NUM,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIA.SET_OF_BOOKS_ID as LEDGER_ID_KEY,
	AIA.INVOICE_CURRENCY_CODE,
	AIA.PAYMENT_CURRENCY_CODE,
	AIA.INVOICE_AMOUNT,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIA.VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
	AIA.AMOUNT_PAID,
	AIA.DISCOUNT_AMOUNT_TAKEN,
	AIA.INVOICE_DATE,
	AIA.SOURCE,
	AIA.INVOICE_TYPE_LOOKUP_CODE,
	AIA.DESCRIPTION,
	AIA.TAX_AMOUNT,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIA.TERMS_ID as TERMS_ID_KEY,	
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
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIA.PROJECT_ID as PROJECT_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIA.PROJECT_ID||'-'||AIA.TASK_ID as PROJECT_TASK_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIA.ORG_ID as ORG_ID_KEY,
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
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AILA.INVENTORY_ITEM_ID as INVENTORY_ITEM_ID_KEY,
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
  --  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AILA.PROJECT_ID as PROJECT_ID_KEY,
  --(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AILA.PROJECT_ID||'-'||AILA.TASK_ID  as PROJECT_TASK_ID_KEY,	
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
	CAST(NVL(AIA.invoice_amount,0) * NVL(DCR.conversion_rate,1) AS DECIMAL(18,2)) GBL_INVOICE_AMOUNT,
    CAST(NVL(AIA.amount_paid,0) * NVL(DCR.conversion_rate,1) AS DECIMAL(18,2)) GBL_AMOUNT_PAID,
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