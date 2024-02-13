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

drop table if exists bec_dwh.FACT_AR_INVOICES;

create table bec_dwh.FACT_AR_INVOICES diststyle all sortkey (CUSTOMER_TRX_ID)
as (
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
    || '-' || nvl(T.CUSTOMER_TRX_ID, 0) as dw_load_id,
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
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ar_invoices'
	and batch_name = 'ar';

commit;