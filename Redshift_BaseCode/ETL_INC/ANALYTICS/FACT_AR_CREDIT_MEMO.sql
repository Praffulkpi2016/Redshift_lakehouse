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
	bec_dwh.FACT_AR_CREDIT_MEMO
where
	(nvl(RECEIVABLE_APPLICATION_ID,0),
	nvl(CM_DIST_LINE_ID,0)) in (
	select
		nvl(ods.RECEIVABLE_APPLICATION_ID,0) as RECEIVABLE_APPLICATION_ID,
		nvl(ods.CM_DIST_LINE_ID,0) as CM_DIST_LINE_ID
	from
		bec_dwh.FACT_AR_CREDIT_MEMO dw,
		(
		select
			AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID,
			AR_DISTRIBUTIONS_ALL.LINE_ID as CM_DIST_LINE_ID
		from
			bec_ods.AR_RECEIVABLE_APPLICATIONS_ALL AR_RECEIVABLE_APPLICATIONS_ALL 
		inner join bec_ods.AR_DISTRIBUTIONS_ALL AR_DISTRIBUTIONS_ALL  
on
			AR_DISTRIBUTIONS_ALL.SOURCE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID
		where
			AR_RECEIVABLE_APPLICATIONS_ALL.APPLICATION_TYPE = 'CM'
			and NVL(AR_RECEIVABLE_APPLICATIONS_ALL.CONFIRMED_FLAG, 'Y') = 'Y'
				and AR_DISTRIBUTIONS_ALL.SOURCE_TABLE = 'RA'
				and AR_DISTRIBUTIONS_ALL.SOURCE_TYPE = 'REC'
				and (AR_RECEIVABLE_APPLICATIONS_ALL.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ar_credit_memo'
				and batch_name = 'ar')
				or AR_RECEIVABLE_APPLICATIONS_ALL.is_deleted_flg = 'Y'
				or AR_DISTRIBUTIONS_ALL.is_deleted_flg = 'Y')
		) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS'
)|| '-' || nvl(ods.RECEIVABLE_APPLICATION_ID, 0) || '-' || nvl(ods.CM_DIST_LINE_ID, 0)
			
);

commit;
-- Insert records

insert
	into
	bec_dwh.FACT_AR_CREDIT_MEMO 
(
receipt_app
,receivable_application_id
,last_update_date
,cm_applied_amt
,cm_applied_gl_date
,ledger_id
,cm_apply_date
,cm_applied_post_date
,org_id_key
,org_id
,credit_memo_info
,cm_trx_id
,cm_trx_number
,cm_type_id_key
,cm_type_id
,cm_trx_date
,cm_bill_to_cust_id
,cm_bill_site_use_id
,cm_ship_site_use_id
,cm_currency_code
,receipt_method_id
,cm_amount
,payment_schedule_id
,cm_due_date
,cm_status
,invoice_info
,inv_trx_id
,inv_trx_number
,invoice_amount
,inv_type_id
,inv_trx_date
,inv_bill_to_contact_id_key
,inv_sold_to_site_use_id_key
,inv_bill_to_customer_id_key
,inv_bill_site_use_id_key
,inv_ship_site_use_id_key
,inv_term_id_key
,inv_sales_rep_id_key
,inv_sales_rep_id
,inv_term_id
,inv_ship_to_customer_id
,inv_po
,inv_territory
,interface_header_attribute1
,interface_header_context
,inv_pay_site_use_id
,inv_pay_schedule_id
,inv_due_date
,inv_status
,inv_cust_site_use_id_key
,terms_sequence_number
,distribution_info
,cm_dist_line_id
,cm_dist_cc_id_key
,cm_amt_debit
,cm_amt_credit
,cm_acct_amt_debit
,cm_acct_amt_credit
,cm_legal_entity_id
,inv_legal_entity_id
,proj_num
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
	)
(
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
INNER JOIN (select * from bec_ods.AR_DISTRIBUTIONS_ALL where is_deleted_flg<>'Y')AR_DISTRIBUTIONS_ALL ON AR_DISTRIBUTIONS_ALL.SOURCE_ID= AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID
INNER JOIN (select * from bec_ods.RA_CUSTOMER_TRX_ALL where is_deleted_flg<>'Y')RA_CUSTOMER_TRX_ALL ON RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_RECEIVABLE_APPLICATIONS_ALL.CUSTOMER_TRX_ID 
INNER JOIN (select * from bec_ods.AR_PAYMENT_SCHEDULES_ALL where is_deleted_flg<>'Y') AR_PAYMENT_SCHEDULES_ALL1 ON AR_PAYMENT_SCHEDULES_ALL1.PAYMENT_SCHEDULE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.PAYMENT_SCHEDULE_ID 
INNER JOIN (select * from bec_ods.RA_CUSTOMER_TRX_ALL where is_deleted_flg<>'Y') RA_CUSTOMER_TRX_ALL1 ON RA_CUSTOMER_TRX_ALL1.CUSTOMER_TRX_ID = AR_RECEIVABLE_APPLICATIONS_ALL.APPLIED_CUSTOMER_TRX_ID 
INNER JOIN (select * from bec_ods.AR_PAYMENT_SCHEDULES_ALL where is_deleted_flg<>'Y')AR_PAYMENT_SCHEDULES_ALL ON AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.APPLIED_PAYMENT_SCHEDULE_ID 
WHERE  AR_RECEIVABLE_APPLICATIONS_ALL.APPLICATION_TYPE = 'CM' 
AND NVL(AR_RECEIVABLE_APPLICATIONS_ALL.CONFIRMED_FLAG, 'Y') = 'Y' 
AND AR_DISTRIBUTIONS_ALL.SOURCE_TABLE='RA' 
AND AR_DISTRIBUTIONS_ALL.SOURCE_TYPE='REC' 
and (AR_RECEIVABLE_APPLICATIONS_ALL.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ar_credit_memo'
				and batch_name = 'ar'))
);
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_ar_credit_memo'
	and batch_name = 'ar';

COMMIT;