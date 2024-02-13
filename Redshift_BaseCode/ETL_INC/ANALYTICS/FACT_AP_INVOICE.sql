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
	bec_dwh.fact_ap_invoice
where
	INVOICE_ID in (
	select
		ods.INVOICE_ID
	from
		bec_dwh.fact_ap_invoice dw,
		bec_ods.AP_INVOICES_ALL ODS
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.INVOICE_ID, 0)
			and (ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_invoice'
				and batch_name = 'ap')
				or ods.is_deleted_flg = 'Y')
);

commit;
-- Insert records

insert
	into
	bec_dwh.FACT_AP_INVOICE 
(
	INVOICE_ID_KEY,
	VENDOR_ID_KEY,
	VENDOR_SITE_ID_KEY,
	LEDGER_ID_KEY,
	BATCH_ID_KEY,
	INVOICE_NUM,
	INVOICE_AMOUNT,
	AMOUNT_PAID,
	DISCOUNT_AMOUNT_TAKEN,
	INVOICE_DATE,
	INVOICE_ID,
	TERMS_ID_KEY,
	TERMS_DATE,
	PO_HEADER_ID,
	FREIGHT_AMOUNT,
	INVOICE_RECEIVED_DATE,
	EXCHANGE_RATE,
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
	AMOUNT_APPLICABLE_TO_DISCOUNT,
	GL_ACCOUNT_ID_KEY,
	INVOICE_BASE_AMOUNT,
	ORIGINAL_PREPAYMENT_AMOUNT,
	GBL_INVOICE_AMOUNT,
	GBL_AMOUNT_PAID,
	terms_id,
	approval_status_code,
	VALIDATE_CNT,
	VALIDATE_CNT1,
	NVALIDATE_CNT,
	REVAL_CNT,
	STOPPED_CNT,
	hold_cnt,
	approver_name,
	aproval_action_date,
	IS_DELETED_FLG,
	SOURCE_APP_ID,
	DW_LOAD_ID,
	DW_INSERT_DATE,
	DW_UPDATE_DATE,
	invoice_status)
(
SELECT inv.*,
      (CASE
	    WHEN cancelled_Date is not null THEN
		    'Cancelled'
        WHEN (VALIDATE_CNT+VALIDATE_CNT1 > 0 AND NVALIDATE_CNT = 0 AND  REVAL_CNT = 0 AND STOPPED_CNT =0 AND hold_cnt = 0) THEN 
		   'Validated'
        WHEN (VALIDATE_CNT+VALIDATE_CNT1 = 0 AND NVALIDATE_CNT > 0 AND  REVAL_CNT = 0 AND STOPPED_CNT =0) THEN 		
           'Never Validated'
		WHEN (VALIDATE_CNT+VALIDATE_CNT1 = 0 AND NVALIDATE_CNT = 0 AND  REVAL_CNT = 0 AND STOPPED_CNT =0 ) THEN 		
           'Never Validated'
        WHEN (VALIDATE_CNT+VALIDATE_CNT1 = 0 OR (NVALIDATE_CNT > 0 OR  REVAL_CNT > 0 OR hold_cnt >0) OR STOPPED_CNT = 0 ) THEN 
           'Needs Revalidation'
	    ELSE
		   'Never Validated'
      END) invoice_status	  
	  from (
	select
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || FAP.INVOICE_ID as INVOICE_ID_KEY,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || FAP.VENDOR_ID as VENDOR_ID_KEY,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || FAP.VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || FAP.SET_OF_BOOKS_ID as LEDGER_ID_KEY,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(FAP.BATCH_ID, 0) as BATCH_ID_KEY,
		FAP.INVOICE_NUM,
		FAP.INVOICE_AMOUNT,
		FAP.AMOUNT_PAID,
		FAP.DISCOUNT_AMOUNT_TAKEN,
		FAP.INVOICE_DATE,
		FAP.INVOICE_ID,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || FAP.TERMS_ID as TERMS_ID_KEY,
		FAP.TERMS_DATE,
		FAP.PO_HEADER_ID,
		FAP.FREIGHT_AMOUNT,
		FAP.INVOICE_RECEIVED_DATE,
		FAP.EXCHANGE_RATE,
		FAP.EXCHANGE_DATE,
		FAP.CANCELLED_DATE,
		FAP.CREATION_DATE,
		FAP.LAST_UPDATE_DATE,
		FAP.CANCELLED_AMOUNT,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || FAP.PROJECT_ID as PROJECT_ID_KEY,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || FAP.PROJECT_ID || '-' || TASK_ID as PROJECT_TASK_ID_KEY,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || FAP.ORG_ID as ORG_ID_KEY,
		FAP.ORG_ID as ORG_ID,	
		FAP.PAY_CURR_INVOICE_AMOUNT,
		FAP.GL_DATE,
		FAP.TOTAL_TAX_AMOUNT,
		FAP.LEGAL_ENTITY_ID,
		FAP.PARTY_ID,
		FAP.PARTY_SITE_ID,
		FAP.AMOUNT_APPLICABLE_TO_DISCOUNT,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || FAP.ACCTS_PAY_CODE_COMBINATION_ID as GL_ACCOUNT_ID_KEY,
		NVL(FAP.BASE_AMOUNT, FAP.INVOICE_AMOUNT) "INVOICE_BASE_AMOUNT",
		FAP.ORIGINAL_PREPAYMENT_AMOUNT,
		cast(NVL(FAP.invoice_amount, 0) * NVL(DCR.conversion_rate, 1) as DECIMAL(18, 2)) GBL_INVOICE_AMOUNT,
		cast(NVL(FAP.amount_paid, 0) * NVL(DCR.conversion_rate, 1) as DECIMAL(18, 2)) GBL_AMOUNT_PAID,
		--FAP.terms_date,
	fap.terms_id,
	fap.WFAPPROVAL_STATUS approval_status_code, --AP_WFAPPROVAL_STATUS
	(SELECT count(1)
     FROM bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AID
     WHERE AID.INVOICE_ID = FAP.INVOICE_ID
     AND MATCH_STATUS_FLAG = 'A') VALIDATE_CNT,
	 (SELECT count(1)
     FROM bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AID
     WHERE AID.INVOICE_ID = FAP.INVOICE_ID
     AND MATCH_STATUS_FLAG = 'T' 
      AND NVL(encumbered_flag,'N')= 'N') VALIDATE_CNT1,
	  (SELECT count(1)
     FROM bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AID
     WHERE AID.INVOICE_ID = FAP.INVOICE_ID
     AND (MATCH_STATUS_FLAG = 'N' or MATCH_STATUS_FLAG  is NULL) ) NVALIDATE_CNT,
	  (SELECT count(1)
     FROM bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AID
     WHERE AID.INVOICE_ID = FAP.INVOICE_ID
      AND MATCH_STATUS_FLAG = 'T' 
      AND NVL(encumbered_flag,'N')= 'Y' ) REVAL_CNT,
	  (SELECT count(1)
     FROM bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AID
     WHERE AID.INVOICE_ID = FAP.INVOICE_ID
      AND MATCH_STATUS_FLAG = 'S' ) STOPPED_CNT,
	(SELECT count(1)
     -- INTO   invoice_holds
      FROM   bec_ods.ap_holds_all
      WHERE  invoice_id = FAP.INVOICE_ID
      AND    release_lookup_code is NULL
	  ) hold_cnt,
	(SELECT approver_name
	   FROM bec_ods.ap_inv_aprvl_hist_all
	   WHERE invoice_id = fap.invoice_id
	   AND approval_history_id  = apv.approval_history_id) approver_name,
	  (SELECT creation_date
	  FROM bec_ods.ap_inv_aprvl_hist_all
	   WHERE invoice_id = fap.invoice_id
	   AND approval_history_id  = apv.approval_history_id) aproval_action_date,
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
       || nvl(FAP.INVOICE_ID, 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
	(select * from bec_ods.AP_INVOICES_ALL where is_deleted_flg <> 'Y') FAP,
    (select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y') DCR,
	(select invoice_id,max(approval_history_id) as approval_history_id from bec_ods.ap_inv_aprvl_hist_all 
	 where is_deleted_flg <> 'Y'
	 group by invoice_id) apv
	where
		1 = 1
		and DCR.to_currency(+) = 'USD'
		and DCR.conversion_type(+) = 'Corporate'
		and FAP.invoice_currency_code = DCR.from_currency(+)
		and DCR.conversion_date(+) = FAP.invoice_date
		AND fap.invoice_id = apv.invoice_id(+)
		and (FAP.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ap_invoice'
				and batch_name = 'ap'))
				) inv
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ap_invoice'
	and batch_name = 'ap';

commit;