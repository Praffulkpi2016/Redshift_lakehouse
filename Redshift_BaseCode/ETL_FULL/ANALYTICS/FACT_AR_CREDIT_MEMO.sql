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

drop table if exists bec_dwh.FACT_AR_CREDIT_MEMO;

create table bec_dwh.FACT_AR_CREDIT_MEMO diststyle all sortkey (RECEIVABLE_APPLICATION_ID,CM_DIST_LINE_ID)
as (
select
'RECEIPT_APP'  Receipt_App,
AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID, 
--AR_RECEIVABLE_APPLICATIONS_ALL.LAST_UPDATED_BY, 
AR_RECEIVABLE_APPLICATIONS_ALL.LAST_UPDATE_DATE, 
--AR_RECEIVABLE_APPLICATIONS_ALL.CREATED_BY, 
--AR_RECEIVABLE_APPLICATIONS_ALL.CREATION_DATE, 
AR_RECEIVABLE_APPLICATIONS_ALL.AMOUNT_APPLIED   CM_Applied_Amt, 
AR_RECEIVABLE_APPLICATIONS_ALL.GL_DATE          CM_Applied_GL_Date, 
AR_RECEIVABLE_APPLICATIONS_ALL.SET_OF_BOOKS_ID  Ledger_ID, 
AR_RECEIVABLE_APPLICATIONS_ALL.APPLY_DATE       CM_Apply_Date, 
AR_RECEIVABLE_APPLICATIONS_ALL.GL_POSTED_DATE   CM_Applied_Post_Date, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AR_RECEIVABLE_APPLICATIONS_ALL.ORG_ID  ORG_ID_KEY, 
AR_RECEIVABLE_APPLICATIONS_ALL.ORG_ID as ORG_ID,
'CREDIT_MEMO_INFO'  Credit_Memo_Info,
RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID             CM_Trx_Id, 
RA_CUSTOMER_TRX_ALL.TRX_NUMBER                  CM_Trx_Number, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID            CM_TYPE_ID_KEY, 
RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID            CM_TYPE_ID,
RA_CUSTOMER_TRX_ALL.TRX_DATE                    CM_Trx_Date, 
RA_CUSTOMER_TRX_ALL.BILL_TO_CUSTOMER_ID         CM_Bill_To_Cust_Id, 
RA_CUSTOMER_TRX_ALL.BILL_TO_SITE_USE_ID         CM_Bill_Site_Use_Id, 
RA_CUSTOMER_TRX_ALL1.SHIP_TO_SITE_USE_ID        CM_SHIP_SITE_USE_ID,
RA_CUSTOMER_TRX_ALL.INVOICE_CURRENCY_CODE       CM_Currency_Code, 
RA_CUSTOMER_TRX_ALL.RECEIPT_METHOD_ID, 
AR_PAYMENT_SCHEDULES_ALL1.AMOUNT_DUE_ORIGINAL   CM_Amount,
AR_PAYMENT_SCHEDULES_ALL1.PAYMENT_SCHEDULE_ID, 
AR_PAYMENT_SCHEDULES_ALL1.DUE_DATE              CM_Due_Date, 
AR_PAYMENT_SCHEDULES_ALL1.STATUS                CM_Status, 
'INVOICE_INFO'   Invoice_Info,
RA_CUSTOMER_TRX_ALL1.CUSTOMER_TRX_ID            Inv_Trx_Id, 
RA_CUSTOMER_TRX_ALL1.TRX_NUMBER                 Inv_Trx_number,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_ORIGINAL    Invoice_Amount,
RA_CUSTOMER_TRX_ALL1.CUST_TRX_TYPE_ID           Inv_Type_Id, 
RA_CUSTOMER_TRX_ALL1.TRX_DATE                   Inv_Trx_Date, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL1.BILL_TO_CONTACT_ID         Inv_Bill_To_Contact_Id_KEY, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||(select CUST_ACCT_SITE_ID from (select * from bec_ods.HZ_CUST_SITE_USES_ALL where is_deleted_flg<>'Y')HZ_CUST_SITE_USES_ALL where site_use_id =RA_CUSTOMER_TRX_ALL1.SOLD_TO_SITE_USE_ID ) || '-' ||RA_CUSTOMER_TRX_ALL1.SOLD_TO_SITE_USE_ID       INV_SOLD_TO_SITE_USE_ID_KEY, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL1.BILL_TO_CUSTOMER_ID        INV_BILL_TO_CUSTOMER_ID_KEY,  
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL1.BILL_TO_SITE_USE_ID       INV_BILL_SITE_USE_ID_KEY, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL1.SHIP_TO_SITE_USE_ID         INV_SHIP_SITE_USE_ID_KEY, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL1.TERM_ID                    INV_TERM_ID_KEY, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RA_CUSTOMER_TRX_ALL1.PRIMARY_SALESREP_ID        INV_SALES_REP_ID_KEY, 
RA_CUSTOMER_TRX_ALL1.PRIMARY_SALESREP_ID        INV_SALES_REP_ID, 
RA_CUSTOMER_TRX_ALL1.TERM_ID                    INV_TERM_ID,
RA_CUSTOMER_TRX_ALL1.SHIP_TO_CUSTOMER_ID        INV_SHIP_TO_CUSTOMER_ID,
RA_CUSTOMER_TRX_ALL1.PURCHASE_ORDER             Inv_PO, 
RA_CUSTOMER_TRX_ALL1.TERRITORY_ID               Inv_Territory, 
RA_CUSTOMER_TRX_ALL1.INTERFACE_HEADER_ATTRIBUTE1, 
RA_CUSTOMER_TRX_ALL1.INTERFACE_HEADER_CONTEXT, 
RA_CUSTOMER_TRX_ALL1.PAYING_SITE_USE_ID         Inv_Pay_Site_Use_Id, 
AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID    Inv_Pay_Schedule_Id, 
AR_PAYMENT_SCHEDULES_ALL.DUE_DATE               Inv_Due_Date, 
AR_PAYMENT_SCHEDULES_ALL.STATUS                 Inv_Status, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_SITE_USE_ID   INV_CUST_SITE_USE_ID_KEY, 
AR_PAYMENT_SCHEDULES_ALL.TERMS_SEQUENCE_NUMBER, 

'DISTRIBUTION_INFO'  Distribution_Info,
AR_DISTRIBUTIONS_ALL.LINE_ID                    CM_DIST_LINE_ID, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AR_DISTRIBUTIONS_ALL.CODE_COMBINATION_ID        CM_DIST_CC_ID_KEY, 
AR_DISTRIBUTIONS_ALL.AMOUNT_DR                  CM_AMT_Debit, 
AR_DISTRIBUTIONS_ALL.AMOUNT_CR                  CM_AMT_Credit, 
AR_DISTRIBUTIONS_ALL.ACCTD_AMOUNT_DR            CM_Acct_AMT_Debit, 
AR_DISTRIBUTIONS_ALL.ACCTD_AMOUNT_CR            CM_Acct_AMT_Credit, 
--AR_DISTRIBUTIONS_ALL.LAST_UPDATE_DATE, 
--AR_PAYMENT_SCHEDULES_ALL.LAST_UPDATE_DATE, 
--RA_CUSTOMER_TRX_ALL.LAST_UPDATE_DATE,
RA_CUSTOMER_TRX_ALL.LEGAL_ENTITY_ID             CM_Legal_Entity_Id,
RA_CUSTOMER_TRX_ALL1.LEGAL_ENTITY_ID            Inv_Legal_Entity_Id,
RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_ATTRIBUTE1 AS PROJ_NUM,
'N' as is_deleted_flg,
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
       || nvl(AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID, 0) 
	|| '-'|| nvl(AR_DISTRIBUTIONS_ALL.LINE_ID,0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM  (select * from bec_ods.AR_RECEIVABLE_APPLICATIONS_ALL where is_deleted_flg<>'Y')AR_RECEIVABLE_APPLICATIONS_ALL 
INNER JOIN (select * from bec_ods.AR_DISTRIBUTIONS_ALL where is_deleted_flg<>'Y') AR_DISTRIBUTIONS_ALL ON AR_DISTRIBUTIONS_ALL.SOURCE_ID= AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID
INNER JOIN (select * from bec_ods.RA_CUSTOMER_TRX_ALL where is_deleted_flg<>'Y') RA_CUSTOMER_TRX_ALL ON RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_RECEIVABLE_APPLICATIONS_ALL.CUSTOMER_TRX_ID 
INNER JOIN (select * from bec_ods.AR_PAYMENT_SCHEDULES_ALL where is_deleted_flg<>'Y') AR_PAYMENT_SCHEDULES_ALL1 ON AR_PAYMENT_SCHEDULES_ALL1.PAYMENT_SCHEDULE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.PAYMENT_SCHEDULE_ID 
INNER JOIN (select * from bec_ods.RA_CUSTOMER_TRX_ALL where is_deleted_flg<>'Y') RA_CUSTOMER_TRX_ALL1 ON RA_CUSTOMER_TRX_ALL1.CUSTOMER_TRX_ID = AR_RECEIVABLE_APPLICATIONS_ALL.APPLIED_CUSTOMER_TRX_ID 
INNER JOIN (select * from bec_ods.AR_PAYMENT_SCHEDULES_ALL where is_deleted_flg<>'Y')AR_PAYMENT_SCHEDULES_ALL ON AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.APPLIED_PAYMENT_SCHEDULE_ID 
WHERE  AR_RECEIVABLE_APPLICATIONS_ALL.APPLICATION_TYPE = 'CM' 
AND NVL(AR_RECEIVABLE_APPLICATIONS_ALL.CONFIRMED_FLAG, 'Y') = 'Y' 
AND AR_DISTRIBUTIONS_ALL.SOURCE_TABLE='RA' 
AND AR_DISTRIBUTIONS_ALL.SOURCE_TYPE='REC' 

);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ar_credit_memo'
	and batch_name = 'ar';

commit;