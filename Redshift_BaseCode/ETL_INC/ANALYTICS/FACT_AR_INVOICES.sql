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

delete from  bec_dwh.FACT_AR_INVOICES
where ( nvl(CUSTOMER_TRX_ID,0) ) in 
(
select nvl(ods.CUSTOMER_TRX_ID,0)  AS CUSTOMER_TRX_ID
from bec_dwh.FACT_AR_INVOICES dw,
(SELECT   
    T.CUSTOMER_TRX_ID  
	FROM bec_ods.RA_CUSTOMER_TRX_ALL T,
		 bec_ods.RA_CUSTOMER_TRX_ALL PT,
	(SELECT CUSTOMER_TRX_ID,
			CLASS,
			MIN(DUE_DATE) DUE_DATE,
			MIN(GL_DATE) GL_DATE,
			CASE
			WHEN SUM (AMOUNT_DUE_REMAINING) = 0
			  THEN 'Y'
			  ELSE 'N'
			END FULLY_PAID_FLAG,
			is_deleted_flg
			  FROM bec_ods.AR_PAYMENT_SCHEDULES_ALL
			  GROUP BY CUSTOMER_TRX_ID,
				CLASS,is_deleted_flg
	) PS,
  bec_ods.RA_TERMS_TL TM,
  bec_ods.RA_CUST_TRX_TYPES_ALL RT 
WHERE 1=1
AND T.PREVIOUS_CUSTOMER_TRX_ID = PT.CUSTOMER_TRX_ID(+)
AND T.CUST_TRX_TYPE_ID    = RT.CUST_TRX_TYPE_ID
AND T.ORG_ID              = RT.ORG_ID
AND T.TERM_ID             = TM.TERM_ID(+)
AND T.CUSTOMER_TRX_ID     = PS.CUSTOMER_TRX_ID(+)
AND TM.LANGUAGE           = 'US'	
and (T.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ar_invoices'
				and batch_name = 'ar')
	or T.is_deleted_flg = 'Y'
	or PT.is_deleted_flg = 'Y'
	or PS.is_deleted_flg = 'Y'
	or TM.is_deleted_flg = 'Y'
	or RT.is_deleted_flg = 'Y')
)ods
where dw.dw_load_id =(
			select
				system_id
			from
				bec_etl_ctrl.etlsourceappid
			where
				source_system = 'EBS'
		)
		|| '-' || nvl(ods.CUSTOMER_TRX_ID, 0) 
);
COMMIT;
-- Insert records

insert
	into
	bec_dwh.fact_ar_invoices
(	customer_trx_id,
	trx_number,
	ledger_id_key,
	org_id_key,
	org_id,
	bill_to_customer_id_key,
	cust_bill_to_site_use_id_key,
	cust_ship_to_customer_id_key,
	bill_to_customer_id,
	bill_to_site_use_id,
	ship_to_customer_id,
	ship_to_site_use_id,
	cust_ship_to_site_use_id_key,
	trx_date,
	invoice_due_date,
	invoice_gl_date,
	invoice_comments,
	payment_term,
	payment_term_description,
	purchase_order,
	invoice_currency_code,
	last_update_date,
	trx_type_name,
	trx_type_description,
	trx_type,
	old_trx_number,
	ct_reference,
	original_invoice_number,
	original_invoice_date,
	fully_paid_flag,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date)

(
	
SELECT T.CUSTOMER_TRX_ID,
  T.TRX_NUMBER,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||T.SET_OF_BOOKS_ID LEDGER_ID_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||T.ORG_ID             ORG_ID_KEY,
  T.ORG_ID as ORG_ID,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||T.BILL_TO_CUSTOMER_ID Bill_to_customer_id_key,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') || '-' || T.BILL_TO_SITE_USE_ID CUST_BILL_TO_SITE_USE_ID_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||T.SHIP_TO_CUSTOMER_ID 	CUST_SHIP_TO_CUSTOMER_ID_KEY,
  T.BILL_TO_CUSTOMER_ID,
  T.BILL_TO_SITE_USE_ID,
  T.SHIP_TO_CUSTOMER_ID,
  T.SHIP_TO_SITE_USE_ID,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') || '-' || T.SHIP_TO_SITE_USE_ID CUST_SHIP_TO_SITE_USE_ID_KEY,
  T.TRX_DATE,
  CASE
    WHEN PS.CLASS = 'INV'
    THEN PS.DUE_DATE
  END INVOICE_DUE_DATE,
  CASE
    WHEN PS.CLASS = 'INV'
    THEN PS.GL_DATE
  END INVOICE_GL_DATE,
  T.COMMENTS INVOICE_COMMENTS,
  TM.NAME PAYMENT_TERM,
  TM.DESCRIPTION PAYMENT_TERM_DESCRIPTION,
  T.PURCHASE_ORDER,
  T.INVOICE_CURRENCY_CODE,
  T.LAST_UPDATE_DATE,
  RT.NAME TRX_TYPE_NAME,
  RT.DESCRIPTION TRX_TYPE_DESCRIPTION,
  RT.TYPE TRX_TYPE ,
  T.OLD_TRX_NUMBER,
  T.CT_REFERENCE,
  PT.TRX_NUMBER ORIGINAL_INVOICE_NUMBER,
  PT.TRX_DATE ORIGINAL_INVOICE_DATE,
  PS.FULLY_PAID_FLAG,
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
       || nvl(T.CUSTOMER_TRX_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM (select * from bec_ods.RA_CUSTOMER_TRX_ALL where is_deleted_flg <> 'Y') T,
  (select * from bec_ods.RA_CUSTOMER_TRX_ALL where is_deleted_flg <> 'Y') PT,
  (SELECT CUSTOMER_TRX_ID,
    CLASS,
    MIN(DUE_DATE) DUE_DATE,
    MIN(GL_DATE) GL_DATE,
    CASE
      WHEN SUM (AMOUNT_DUE_REMAINING) = 0
      THEN 'Y'
      ELSE 'N'
    END FULLY_PAID_FLAG
  FROM (select * from bec_ods.AR_PAYMENT_SCHEDULES_ALL where is_deleted_flg <> 'Y')AR_PAYMENT_SCHEDULES_ALL
  GROUP BY CUSTOMER_TRX_ID,
    CLASS
  ) PS,
  (select * from bec_ods.RA_TERMS_TL where is_deleted_flg <> 'Y') TM,
  (select * from bec_ods.RA_CUST_TRX_TYPES_ALL where is_deleted_flg <> 'Y') RT 
WHERE 1               =1
AND T.PREVIOUS_CUSTOMER_TRX_ID = PT.CUSTOMER_TRX_ID(+)
AND T.CUST_TRX_TYPE_ID    = RT.CUST_TRX_TYPE_ID
AND T.ORG_ID              = RT.ORG_ID
AND T.TERM_ID             = TM.TERM_ID(+)
AND T.CUSTOMER_TRX_ID     = PS.CUSTOMER_TRX_ID(+)
AND TM.LANGUAGE           = 'US' 
and (T.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ar_invoices'
				and batch_name = 'ar'))
);
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_ar_invoices'
	and batch_name = 'ar';

COMMIT;