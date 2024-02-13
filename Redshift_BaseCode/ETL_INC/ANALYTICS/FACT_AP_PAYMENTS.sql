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
	bec_dwh.fact_ap_payments
where
	(nvl(INVOICE_ID, 0),
	nvl(INVOICE_PAYMENT_ID, 0),
	nvl(PAYMENT_NUM, 0)) in (
	select
		nvl(ods.INVOICE_ID, 0) as INVOICE_ID,
		nvl(ods.INVOICE_PAYMENT_ID, 0) as INVOICE_PAYMENT_ID,
		nvl(ods.PAYMENT_NUM, 0) as PAYMENT_NUM
	from
		bec_dwh.fact_ap_payments dw,
		(
		select
			(
			select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||APSCA.INVOICE_ID as INVOICE_ID_KEY,
	APSCA.PAYMENT_NUM,
	APSCA.AMOUNT_REMAINING,
	APSCA.DISCOUNT_DATE,
	APSCA.DUE_DATE,
	--APSCA.FUTURE_PAY_DUE_DATE,
	APSCA.GROSS_AMOUNT,
	APSCA.PAYMENT_METHOD_CODE PAYMENT_METHOD_LOOKUP_CODE,
	APSCA.HOLD_FLAG,
	APSCA.PAYMENT_STATUS_FLAG,
	APSCA.SECOND_DISCOUNT_DATE,
	APSCA.THIRD_DISCOUNT_DATE,
	APSCA.DISCOUNT_AMOUNT_REMAINING,
	APSCA.SECOND_DISC_AMT_AVAILABLE,
	APSCA.THIRD_DISC_AMT_AVAILABLE,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||APSCA.ORG_ID as ORG_ID_KEY,
	APSCA.ORG_ID as ORG_ID,
	APSCA.IBY_HOLD_REASON,
	APSCA.REMIT_TO_SUPPLIER_NAME,
	APSCA.REMIT_TO_SUPPLIER_ID,
	APSCA.REMIT_TO_SUPPLIER_SITE,
	APSCA.REMIT_TO_SUPPLIER_SITE_ID,
	APSCA.INVOICE_ID,
	AIPA.ACCOUNTING_DATE,
	AIPA.AMOUNT,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIPA.CHECK_ID as CHECK_ID_KEY,
	AIPA.INVOICE_PAYMENT_ID,
	AIPA.PERIOD_NAME,
	AIPA.POSTED_FLAG,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIPA.SET_OF_BOOKS_ID as LEDGER_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIPA.ACCTS_PAY_CODE_COMBINATION_ID as GL_ACCOUNT_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIA.TERMS_ID as TERMS_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||VENDOR_ID as VENDOR_ID_KEY,
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
	AIPA.ASSET_CODE_COMBINATION_ID,
	AIPA.BANK_ACCOUNT_NUM,
	AIPA.BANK_ACCOUNT_TYPE,
	AIPA.BANK_NUM,
	AIPA.DISCOUNT_LOST,
	AIPA.DISCOUNT_TAKEN,
	AIPA.EXCHANGE_DATE,
	AIPA.EXCHANGE_RATE,
	AIPA.EXCHANGE_RATE_TYPE,
	AIPA.INVOICE_BASE_AMOUNT "INV_AMT_FUNC_CURR",
	AIPA.PAYMENT_BASE_AMOUNT "PAY_AMT_FUNC_CURR",
	AIPA.ORG_ID "Payment Org",
	AIPA.ACCOUNTING_EVENT_ID,
	AIPA.REVERSAL_FLAG,
	AIPA.REVERSAL_INV_PMT_ID,
	AIA.INVOICE_AMOUNT  ,
	AIA.AMOUNT_PAID,
	    cast(NVL(AIA.invoice_amount,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INVOICE_AMOUNT,
    cast(NVL(AIA.amount_paid,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT_PAID,	
	cast(NVL(AIPA.AMOUNT,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_PAY_AMOUNT,
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
       || nvl(APSCA.INVOICE_ID, 0) || '-' || nvl(AIPA.INVOICE_PAYMENT_ID, 0) || '-' || nvl(APSCA.PAYMENT_NUM, 0) as dw_load_id,
			getdate() as dw_insert_date,
			getdate() as dw_update_date
		from
	bec_ods.AP_INVOICE_PAYMENTS_ALL AIPA,
	bec_ods.AP_PAYMENT_SCHEDULES_ALL APSCA,
	bec_ods.AP_INVOICES_ALL AIA,
	bec_ods.GL_DAILY_RATES DCR
		where
	1 = 1
	and AIA.INVOICE_ID = APSCA.INVOICE_ID
	and AIPA.INVOICE_ID(+)= APSCA.INVOICE_ID
	and DCR.to_currency(+) = 'USD'
	and DCR.conversion_type(+) = 'Corporate'
	and AIA.invoice_currency_code = DCR.from_currency(+)
	and DCR.conversion_date(+) = AIA.invoice_date
			and (AIPA.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_payments'
				and batch_name = 'ap')
			or
				APSCA.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_payments'
				and batch_name = 'ap')
			or AIPA.is_deleted_flg = 'Y'
			or APSCA.is_deleted_flg = 'Y'
			or AIA.is_deleted_flg = 'Y'
			or DCR.is_deleted_flg = 'Y')
) ods
	where
		dw.dw_load_id = 
	(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS'
    )
    || '-'
       || nvl(ods.INVOICE_ID, 0) || '-' || nvl(ods.INVOICE_PAYMENT_ID, 0) || '-' || nvl(ods.PAYMENT_NUM, 0) 
);

commit;
-- Insert records

insert
	into
	bec_dwh.fact_ap_payments
(
INVOICE_ID_KEY,
	PAYMENT_NUM,
	AMOUNT_REMAINING,
	DISCOUNT_DATE,
	DUE_DATE,
	GROSS_AMOUNT,
	PAYMENT_METHOD_LOOKUP_CODE,
	HOLD_FLAG,
	PAYMENT_STATUS_FLAG,
	SECOND_DISCOUNT_DATE,
	THIRD_DISCOUNT_DATE,
	DISCOUNT_AMOUNT_REMAINING,
	SECOND_DISC_AMT_AVAILABLE,
	THIRD_DISC_AMT_AVAILABLE,
	ORG_ID_KEY,
	ORG_ID,
	IBY_HOLD_REASON,
	REMIT_TO_SUPPLIER_NAME,
	REMIT_TO_SUPPLIER_ID,
	REMIT_TO_SUPPLIER_SITE,
	REMIT_TO_SUPPLIER_SITE_ID,
	INVOICE_ID,
	ACCOUNTING_DATE,
	AMOUNT,
	CHECK_ID_KEY,
	INVOICE_PAYMENT_ID,
	PERIOD_NAME,
	POSTED_FLAG,
	LEDGER_ID_KEY,
	GL_ACCOUNT_ID_KEY,
	TERMS_ID_KEY,
	VENDOR_ID_KEY,
	VENDOR_SITE_ID_KEY,
	ASSET_CODE_COMBINATION_ID,
	BANK_ACCOUNT_NUM,
	BANK_ACCOUNT_TYPE,
	BANK_NUM,
	DISCOUNT_LOST,
	DISCOUNT_TAKEN,
	EXCHANGE_DATE,
	EXCHANGE_RATE,
	EXCHANGE_RATE_TYPE,
	"INV_AMT_FUNC_CURR",
	"PAY_AMT_FUNC_CURR",
	"Payment Org",
	ACCOUNTING_EVENT_ID,
	REVERSAL_FLAG,
	REVERSAL_INV_PMT_ID,
	INVOICE_AMOUNT ,
	AMOUNT_PAID,
	GBL_INVOICE_AMOUNT,
	GBL_AMOUNT_PAID,
	GBL_PAY_AMOUNT,
	IS_DELETED_FLG,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
		(
		select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||APSCA.INVOICE_ID as INVOICE_ID_KEY,
	APSCA.PAYMENT_NUM,
	APSCA.AMOUNT_REMAINING,
	APSCA.DISCOUNT_DATE,
	APSCA.DUE_DATE,
	--APSCA.FUTURE_PAY_DUE_DATE,
	APSCA.GROSS_AMOUNT,
	APSCA.PAYMENT_METHOD_CODE PAYMENT_METHOD_LOOKUP_CODE,
	APSCA.HOLD_FLAG,
	APSCA.PAYMENT_STATUS_FLAG,
	APSCA.SECOND_DISCOUNT_DATE,
	APSCA.THIRD_DISCOUNT_DATE,
	APSCA.DISCOUNT_AMOUNT_REMAINING,
	APSCA.SECOND_DISC_AMT_AVAILABLE,
	APSCA.THIRD_DISC_AMT_AVAILABLE,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||APSCA.ORG_ID as ORG_ID_KEY,
	APSCA.ORG_ID as ORG_ID,
	APSCA.IBY_HOLD_REASON,
	APSCA.REMIT_TO_SUPPLIER_NAME,
	APSCA.REMIT_TO_SUPPLIER_ID,
	APSCA.REMIT_TO_SUPPLIER_SITE,
	APSCA.REMIT_TO_SUPPLIER_SITE_ID,
	APSCA.INVOICE_ID,
	AIPA.ACCOUNTING_DATE,
	AIPA.AMOUNT,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIPA.CHECK_ID as CHECK_ID_KEY,
	AIPA.INVOICE_PAYMENT_ID,
	AIPA.PERIOD_NAME,
	AIPA.POSTED_FLAG,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIPA.SET_OF_BOOKS_ID as LEDGER_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIPA.ACCTS_PAY_CODE_COMBINATION_ID as GL_ACCOUNT_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AIA.TERMS_ID as TERMS_ID_KEY,
	    (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||VENDOR_ID as VENDOR_ID_KEY,
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
	AIPA.ASSET_CODE_COMBINATION_ID,
	AIPA.BANK_ACCOUNT_NUM,
	AIPA.BANK_ACCOUNT_TYPE,
	AIPA.BANK_NUM,
	AIPA.DISCOUNT_LOST,
	AIPA.DISCOUNT_TAKEN,
	AIPA.EXCHANGE_DATE,
	AIPA.EXCHANGE_RATE,
	AIPA.EXCHANGE_RATE_TYPE,
	AIPA.INVOICE_BASE_AMOUNT "INV_AMT_FUNC_CURR",
	AIPA.PAYMENT_BASE_AMOUNT "PAY_AMT_FUNC_CURR",
	AIPA.ORG_ID "Payment Org",
	AIPA.ACCOUNTING_EVENT_ID,
	AIPA.REVERSAL_FLAG,
	AIPA.REVERSAL_INV_PMT_ID,
	AIA.INVOICE_AMOUNT  ,
	AIA.AMOUNT_PAID,
	    cast(NVL(AIA.invoice_amount,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INVOICE_AMOUNT,
    cast(NVL(AIA.amount_paid,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT_PAID,	
	cast(NVL(AIPA.AMOUNT,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_PAY_AMOUNT,
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
       || nvl(APSCA.INVOICE_ID, 0) || '-' || nvl(AIPA.INVOICE_PAYMENT_ID, 0) || '-' || nvl(APSCA.PAYMENT_NUM, 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
	(select * from bec_ods.AP_INVOICE_PAYMENTS_ALL where is_deleted_flg <> 'Y') AIPA,
	(select * from bec_ods.AP_PAYMENT_SCHEDULES_ALL where is_deleted_flg <> 'Y') APSCA,
	(select * from bec_ods.AP_INVOICES_ALL where is_deleted_flg <> 'Y') AIA,
	(select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y') DCR
	where
	1 = 1
	and AIA.INVOICE_ID = APSCA.INVOICE_ID
	and AIPA.INVOICE_ID(+)= APSCA.INVOICE_ID
	and DCR.to_currency(+) = 'USD'
	and DCR.conversion_type(+) = 'Corporate'
	and AIA.invoice_currency_code = DCR.from_currency(+)
	and DCR.conversion_date(+) = AIA.invoice_date
		and (AIPA.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_payments'
				and batch_name = 'ap')
			or
APSCA.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_payments'
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
	dw_table_name = 'fact_ap_payments'
	and batch_name = 'ap';

commit;