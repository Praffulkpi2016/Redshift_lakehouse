/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental approach for Facts.
# File Version: KPI v1.0
*/

begin;

TRUNCATE TABLE BEC_DWH.FACT_AR_RECEIPTS;

INSERT INTO BEC_DWH.FACT_AR_RECEIPTS 
(
select 
RECEIPT_APP
,OPERATING_UNIT
,RECEIVABLE_APPLICATION_ID
,RECEIVABLE_APPLICATION_ID_KEY
,GL_DATE
,SET_OF_BOOKS_ID
,APPLY_DATE
,APPLICATION_TYPE
,RCV_STATUS
,RCV_AMOUNT_APPLIED
,DISPLAY
,GL_POSTED_DATE
,ORG_ID_KEY
,ORG_ID
,RECEIPT_INFO
,CASH_RECEIPT_ID
,APPLIED_CUSTOMER_TRX_ID
,APPLIED_PAYMENT_SCHEDULE_ID
,RECEIPT_CURRENCY
,PAY_FROM_CUSTOMER
,RECEIPT_NUMBER
,RECEIPT_DATE
,BANK_ACCOUNT_ID
,BANK_ACCT_USE_ID
,RECEIPT_CUST_SITE_USE_ID
,CUST_SITE_USE_ID_KEY
,RECEIPT_METHOD_ID
,AMOUNT_DUE_ORIGINAL
,AMOUNT_DUE_REMAINING
,AMOUNT_APPLIED
,SCHEDULE_ID
,DUE_DATE
,PAYMENT_STATUS
,RECEIPT_LEGAL_ENTITY
,RECEIPT_AMOUNT_F
,EXCHANGE_RATE
,EXCHANGE_RATE_TYPE
,EXCHANGE_RATE_DATE
,REMIT_BANK_ACCT_USE_ID
,INVOICE_INFO
,INVOICE_ID
,INVOICE_ID_KEY
,INVOICE_NUMBER
,INV_TRX_TYPE_ID
,INV_TRX_TYPE_ID_KEY
,INVOICE_DATE
,INV_BILL_TO_CONTACT_ID
,INV_SOLD_TO_SITE_USE_ID
,INV_BILL_TO_CUST_ID
,INV_BILL_TO_CUST_ID_KEY
,INV_BILL_SITE_USE_ID
,INV_SHIP_SITE_USE_ID
,INV_BILL_TO_SITE_USE_ID_KEY
,INV_SHIP_TO_SITE_USE_ID_KEY
,INV_TERM_ID
,INV_TERM_ID_KEY
,INV_SALES_REP_ID
,INV_SALES_REP_ID_KEY
,INV_PO
,INV_AMOUNT
,INV_REMAINING_AMOUNT
,RECEIPT_AMT_APPLIED_ON_INVOICE
,AMOUNT_LINE_ITEMS_ORIGINAL
,AMOUNT_LINE_ITEMS_REMAINING
,INV_AMT_ADJUSTED
,INV_AMT_DISPUTE
,INV_AMT_CREDITTED
,INV_CHARGES_AMT
,INV_CHARGES_REMAINING
,INV_FREIGHT_AMT
,INV_FREIGHT_AMT_REMAINING
,INV_TAX_ORIGINAL
,INV_TAX_REMAINING
,INV_DISCOUNT_EARNED
,INV_DISCOUNT_UNEARNED
,INV_PAY_SCHEDULE_ID
,INV_DUE_DATE
,INVOICE_STATUS
,PROJ_NUM
,INV_TERRITORY
,INTERFACE_HEADER_ATTRIBUTE1
,INTERFACE_HEADER_CONTEXT
,INV_PAY_SITE_USE_ID
,RECEIPT_APPLIED_DISTR_INFO
,DIST_LINE_ID
,RECEIPT_APP_DIST_LINE_ID_KEY,
RECEIPT_APP_CC_ID, 
RECEIPT_APP_AMT_DR,
RECEIPT_APP_AMT_CR,
RECEIPT_APP_AMT_ACCT_DR,
RECEIPT_APP_AMT_ACCT_CR,
SOURCE_ID,
RECEIPT_APPLIED_HISTORY_INFO,
RECEIPT_HISTORY_ID,
RECEIPT_HISTORY_ID_KEY,
AMOUNT
,ACCT_AMOUNT
,CURRENT_FLAG
,REVERSE_GL_DATE
,EXCHANGE_GAIN_LOSS
,SOURCE_NAME
,'N' AS IS_DELETED_FLG
,SOURCE_APP_ID
,DW_LOAD_ID
,DW_INSERT_DATE
,DW_UPDATE_DATE,
DECODE(RCV_STATUS,'APP', 
SUM(RCV_AMOUNT_APPLIED)) AS APPLIED_AMOUNT,
DECODE(RCV_STATUS,'UNAPP', 
SUM(RCV_AMOUNT_APPLIED)) AS UNAPPLIED_AMOUNT
,cast(NVL(AMOUNT_DUE_ORIGINAL,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_AMOUNT_DUE_ORIGINAL
,cast(NVL(AMOUNT_DUE_REMAINING,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_AMOUNT_DUE_REMAINING
,cast(NVL(AMOUNT_APPLIED,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_AMOUNT_APPLIED
,cast(NVL(INV_AMOUNT,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INV_AMOUNT
,cast(NVL(INV_REMAINING_AMOUNT,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INV_REMAINING_AMOUNT
,cast(NVL(INV_FREIGHT_AMT,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INV_FREIGHT_AMT
,cast(NVL(INV_FREIGHT_AMT_REMAINING,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INV_FREIGHT_AMT_REMAINING
,cast(NVL(ACCT_AMOUNT,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_ACCT_AMOUNT
,cast(NVL(AMOUNT,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_AMOUNT
,cast(NVL(RCV_AMOUNT_APPLIED,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_CV_AMOUNT_APPLIED
, (case when RCV_STATUS='UNAPP' then sum(RCV_AMOUNT_APPLIED)*NVL(DCR.conversion_rate,1)::decimal(18,2) end ) GBL_UNAPPLIED_AMOUNT
from (
SELECT 
'RECEIPT_APP' RECEIPT_APP,
AR_RECEIVABLE_APPLICATIONS_ALL.ORG_ID                     OPERATING_UNIT,
AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID  RECEIVABLE_APPLICATION_ID, 
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID  AS RECEIVABLE_APPLICATION_ID_KEY, 
AR_RECEIVABLE_APPLICATIONS_ALL.GL_DATE              , 
AR_RECEIVABLE_APPLICATIONS_ALL.SET_OF_BOOKS_ID            ,
AR_RECEIVABLE_APPLICATIONS_ALL.APPLY_DATE                 , 
AR_RECEIVABLE_APPLICATIONS_ALL.APPLICATION_TYPE           , 
AR_RECEIVABLE_APPLICATIONS_ALL.STATUS  as RCV_STATUS                      	  ,
AR_RECEIVABLE_APPLICATIONS_ALL.AMOUNT_APPLIED as RCV_AMOUNT_APPLIED,
AR_RECEIVABLE_APPLICATIONS_ALL.DISPLAY                    ,
AR_RECEIVABLE_APPLICATIONS_ALL.GL_POSTED_DATE            , 
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||AR_RECEIVABLE_APPLICATIONS_ALL.ORG_ID AS ORG_ID_KEY,
AR_RECEIVABLE_APPLICATIONS_ALL.ORG_ID AS ORG_ID,
'RECEIPT_INFO' RECEIPT_INFO,
AR_RECEIVABLE_APPLICATIONS_ALL.CASH_RECEIPT_ID            CASH_RECEIPT_ID,
AR_RECEIVABLE_APPLICATIONS_ALL.APPLIED_CUSTOMER_TRX_ID ,
AR_RECEIVABLE_APPLICATIONS_ALL.APPLIED_PAYMENT_SCHEDULE_ID  APPLIED_PAYMENT_SCHEDULE_ID,
AR_CASH_RECEIPTS_ALL.CURRENCY_CODE                        RECEIPT_CURRENCY, 
AR_CASH_RECEIPTS_ALL.PAY_FROM_CUSTOMER                , 
AR_CASH_RECEIPTS_ALL.RECEIPT_NUMBER                       RECEIPT_NUMBER, 
AR_CASH_RECEIPTS_ALL.RECEIPT_DATE                         RECEIPT_DATE, 
CE_BANK_ACCT_USES_ALL.BANK_ACCOUNT_ID                  ,
CE_BANK_ACCT_USES_ALL.BANK_ACCT_USE_ID,
AR_CASH_RECEIPTS_ALL.CUSTOMER_SITE_USE_ID                 RECEIPT_CUST_SITE_USE_ID,
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM='EBS')||'-'|| AR_CASH_RECEIPTS_ALL.CUSTOMER_SITE_USE_ID
AS CUST_SITE_USE_ID_KEY,  
AR_CASH_RECEIPTS_ALL.RECEIPT_METHOD_ID, 
AR_PAYMENT_SCHEDULES_ALL1.AMOUNT_DUE_ORIGINAL             ,
AR_PAYMENT_SCHEDULES_ALL1.AMOUNT_DUE_REMAINING            ,
AR_PAYMENT_SCHEDULES_ALL1.AMOUNT_APPLIED                  ,
--DECODE(AR_RECEIVABLE_APPLICATIONS_ALL.STATUS,'APP', 
--SUM(AR_RECEIVABLE_APPLICATIONS_ALL.AMOUNT_APPLIED)) AS APPLIED_AMOUNT,
--DECODE(AR_RECEIVABLE_APPLICATIONS_ALL.STATUS,'UNAPP', 
--SUM(AR_RECEIVABLE_APPLICATIONS_ALL.AMOUNT_APPLIED)) AS UNAPPLIED_AMOUNT,
AR_PAYMENT_SCHEDULES_ALL1.PAYMENT_SCHEDULE_ID             SCHEDULE_ID, 
AR_PAYMENT_SCHEDULES_ALL1.DUE_DATE                        DUE_DATE, 
AR_PAYMENT_SCHEDULES_ALL1.STATUS                          PAYMENT_STATUS, 
AR_CASH_RECEIPTS_ALL.LEGAL_ENTITY_ID                      RECEIPT_LEGAL_ENTITY,
(AR_PAYMENT_SCHEDULES_ALL1.AMOUNT_DUE_ORIGINAL * AR_CASH_RECEIPTS_ALL.EXCHANGE_RATE)  RECEIPT_AMOUNT_F,	
AR_CASH_RECEIPTS_ALL.EXCHANGE_RATE						  EXCHANGE_RATE, 
AR_CASH_RECEIPTS_ALL.EXCHANGE_RATE_TYPE,
AR_CASH_RECEIPTS_ALL.EXCHANGE_DATE as exchange_rate_date,
AR_CASH_RECEIPTS_ALL.REMIT_BANK_ACCT_USE_ID,
'INVOICE_INFO'  INVOICE_INFO,
RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID                       INVOICE_ID,
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID AS INVOICE_ID_KEY, 
RA_CUSTOMER_TRX_ALL.TRX_NUMBER                            INVOICE_NUMBER, 
RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID                      INV_TRX_TYPE_ID,
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID AS INV_TRX_TYPE_ID_KEY,
RA_CUSTOMER_TRX_ALL.TRX_DATE                              INVOICE_DATE, 
RA_CUSTOMER_TRX_ALL.BILL_TO_CONTACT_ID                    INV_BILL_TO_CONTACT_ID, 
RA_CUSTOMER_TRX_ALL.SOLD_TO_SITE_USE_ID                   INV_SOLD_TO_SITE_USE_ID, 
RA_CUSTOMER_TRX_ALL.BILL_TO_CUSTOMER_ID                   INV_BILL_TO_CUST_ID, 
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||RA_CUSTOMER_TRX_ALL.BILL_TO_CUSTOMER_ID                   INV_BILL_TO_CUST_ID_KEY, 
RA_CUSTOMER_TRX_ALL.BILL_TO_SITE_USE_ID                   INV_BILL_SITE_USE_ID, 
RA_CUSTOMER_TRX_ALL.SHIP_TO_SITE_USE_ID                   INV_SHIP_SITE_USE_ID, 
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||RA_CUSTOMER_TRX_ALL.BILL_TO_SITE_USE_ID                   INV_BILL_TO_SITE_USE_ID_KEY, 
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||RA_CUSTOMER_TRX_ALL.SHIP_TO_SITE_USE_ID                   INV_SHIP_TO_SITE_USE_ID_KEY,
RA_CUSTOMER_TRX_ALL.TERM_ID                               INV_TERM_ID,
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||RA_CUSTOMER_TRX_ALL.TERM_ID AS INV_TERM_ID_KEY, 
RA_CUSTOMER_TRX_ALL.PRIMARY_SALESREP_ID                   INV_SALES_REP_ID, 
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||RA_CUSTOMER_TRX_ALL.PRIMARY_SALESREP_ID AS INV_SALES_REP_ID_KEY,
RA_CUSTOMER_TRX_ALL.PURCHASE_ORDER                        INV_PO, 
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_ORIGINAL              INV_AMOUNT,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_REMAINING             INV_REMAINING_AMOUNT,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_APPLIED                   RECEIPT_AMT_APPLIED_ON_INVOICE,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_LINE_ITEMS_ORIGINAL,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_LINE_ITEMS_REMAINING,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_ADJUSTED                  INV_AMT_ADJUSTED,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_IN_DISPUTE                INV_AMT_DISPUTE,
AR_PAYMENT_SCHEDULES_ALL.AMOUNT_CREDITED                  INV_AMT_CREDITTED,
AR_PAYMENT_SCHEDULES_ALL.RECEIVABLES_CHARGES_CHARGED      INV_CHARGES_AMT,
AR_PAYMENT_SCHEDULES_ALL.RECEIVABLES_CHARGES_REMAINING    INV_CHARGES_REMAINING,
AR_PAYMENT_SCHEDULES_ALL.FREIGHT_ORIGINAL                 INV_FREIGHT_AMT,
AR_PAYMENT_SCHEDULES_ALL.FREIGHT_REMAINING                INV_FREIGHT_AMT_REMAINING,
AR_PAYMENT_SCHEDULES_ALL.TAX_ORIGINAL                     INV_TAX_ORIGINAL,
AR_PAYMENT_SCHEDULES_ALL.TAX_REMAINING                    INV_TAX_REMAINING,
AR_PAYMENT_SCHEDULES_ALL.DISCOUNT_TAKEN_EARNED            INV_DISCOUNT_EARNED,
AR_PAYMENT_SCHEDULES_ALL.DISCOUNT_TAKEN_UNEARNED          INV_DISCOUNT_UNEARNED,
AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID              INV_PAY_SCHEDULE_ID, 
AR_PAYMENT_SCHEDULES_ALL.DUE_DATE                         INV_DUE_DATE, 
AR_PAYMENT_SCHEDULES_ALL.STATUS                           INVOICE_STATUS,
RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_ATTRIBUTE1           AS PROJ_NUM,
RA_CUSTOMER_TRX_ALL.TERRITORY_ID                          INV_TERRITORY, 
RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_ATTRIBUTE1, 
RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_CONTEXT, 
RA_CUSTOMER_TRX_ALL.PAYING_SITE_USE_ID                    INV_PAY_SITE_USE_ID,
'RECEIPT_APPLIED_DISTR_INFO'  RECEIPT_APPLIED_DISTR_INFO,
AR_DISTRIBUTIONS_ALL.LINE_ID                              DIST_LINE_ID,
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||AR_DISTRIBUTIONS_ALL.LINE_ID AS RECEIPT_APP_DIST_LINE_ID_KEY,
AR_DISTRIBUTIONS_ALL.CODE_COMBINATION_ID                  RECEIPT_APP_CC_ID, 
AR_DISTRIBUTIONS_ALL.AMOUNT_DR                            RECEIPT_APP_AMT_DR,
AR_DISTRIBUTIONS_ALL.AMOUNT_CR                            RECEIPT_APP_AMT_CR,
AR_DISTRIBUTIONS_ALL.ACCTD_AMOUNT_DR                      RECEIPT_APP_AMT_ACCT_DR,
AR_DISTRIBUTIONS_ALL.ACCTD_AMOUNT_CR                      RECEIPT_APP_AMT_ACCT_CR,
AR_DISTRIBUTIONS_ALL.SOURCE_ID                            SOURCE_ID,
'RECEIPT_APPLIED_HISTORY_INFO' RECEIPT_APPLIED_HISTORY_INFO,
AR_RECEIVABLE_APPLICATIONS_ALL.CASH_RECEIPT_HISTORY_ID	  RECEIPT_HISTORY_ID,
(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||AR_RECEIVABLE_APPLICATIONS_ALL.CASH_RECEIPT_HISTORY_ID AS RECEIPT_HISTORY_ID_KEY,
AR_CASH_RECEIPT_HISTORY_ALL.AMOUNT                        AMOUNT,
AR_CASH_RECEIPT_HISTORY_ALL.ACCTD_AMOUNT                  ACCT_AMOUNT,
AR_CASH_RECEIPT_HISTORY_ALL.CURRENT_RECORD_FLAG           CURRENT_FLAG,
AR_CASH_RECEIPT_HISTORY_ALL.REVERSAL_GL_DATE              REVERSE_GL_DATE,
decode(sign(AR_RECEIVABLE_APPLICATIONS_ALL.applied_payment_schedule_id),-1,null,
AR_RECEIVABLE_APPLICATIONS_ALL.acctd_amount_applied_from -
nvl(AR_RECEIVABLE_APPLICATIONS_ALL.acctd_amount_applied_to, AR_RECEIVABLE_APPLICATIONS_ALL.acctd_amount_applied_from)) 
EXCHANGE_GAIN_LOSS,
ra_batch_sources_all.name AS source_name,
--decode(
--(AR_RECEIVABLE_APPLICATIONS_ALL.applied_payment_schedule_id), 1,null
--(AR_RECEIVABLE_APPLICATIONS_ALL.acctd_amount_applied_from) - 
--nvl(AR_RECEIVABLE_APPLICATIONS_ALL.acctd_amount_applied_to, AR_RECEIVABLE_APPLICATIONS_ALL.acctd_amount_applied_from)
--)     as EXCHANGE_GAIN_LOSS,                                                                                                    ,
	    	'N' AS IS_DELETED_FLG,
(
	SELECT
		SYSTEM_ID
	FROM
		BEC_ETL_CTRL.ETLSOURCEAPPID
	WHERE
		SOURCE_SYSTEM =
		'EBS'
    ) AS SOURCE_APP_ID,
	(
	SELECT
		SYSTEM_ID
	FROM
		BEC_ETL_CTRL.ETLSOURCEAPPID
	WHERE
		SOURCE_SYSTEM = 'EBS'
    )
    || '-'
       || NVL(AR_RECEIVABLE_APPLICATIONS_ALL.CASH_RECEIPT_ID, 0)
       || '-'
       || NVL(AR_DISTRIBUTIONS_ALL.LINE_ID, 0) 
	   ||'-'||nvl(ra_batch_sources_all.name,'NA') AS DW_LOAD_ID,
	GETDATE() AS DW_INSERT_DATE,
	GETDATE() AS DW_UPDATE_DATE
FROM
(select * from bec_ods.AR_RECEIVABLE_APPLICATIONS_ALL where is_deleted_flg <> 'Y') AR_RECEIVABLE_APPLICATIONS_ALL
LEFT OUTER JOIN (select * from bec_ods.RA_CUSTOMER_TRX_ALL where is_deleted_flg <> 'Y')  RA_CUSTOMER_TRX_ALL 
ON RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_RECEIVABLE_APPLICATIONS_ALL.APPLIED_CUSTOMER_TRX_ID
INNER JOIN 
	(select * from bec_ods.AR_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y') AR_DISTRIBUTIONS_ALL 
	ON AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID = AR_DISTRIBUTIONS_ALL.SOURCE_ID
LEFT OUTER JOIN (select * from bec_ods.AR_PAYMENT_SCHEDULES_ALL where is_deleted_flg <> 'Y')  AR_PAYMENT_SCHEDULES_ALL 
ON AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.APPLIED_PAYMENT_SCHEDULE_ID
INNER JOIN (select * from bec_ods.AR_CASH_RECEIPTS_ALL where is_deleted_flg <> 'Y')  AR_CASH_RECEIPTS_ALL 
ON AR_CASH_RECEIPTS_ALL.CASH_RECEIPT_ID = AR_RECEIVABLE_APPLICATIONS_ALL.CASH_RECEIPT_ID 
INNER JOIN (select * from bec_ods.AR_PAYMENT_SCHEDULES_ALL where is_deleted_flg <> 'Y')   AR_PAYMENT_SCHEDULES_ALL1 
ON AR_PAYMENT_SCHEDULES_ALL1.PAYMENT_SCHEDULE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.PAYMENT_SCHEDULE_ID
LEFT OUTER JOIN (select * from bec_ods.CE_BANK_ACCT_USES_ALL where is_deleted_flg <> 'Y')  CE_BANK_ACCT_USES_ALL 
ON AR_CASH_RECEIPTS_ALL.REMIT_BANK_ACCT_USE_ID = CE_BANK_ACCT_USES_ALL.BANK_ACCT_USE_ID
INNER JOIN (select * from bec_ods.AR_CASH_RECEIPT_HISTORY_ALL where is_deleted_flg <> 'Y')  AR_CASH_RECEIPT_HISTORY_ALL 
ON AR_CASH_RECEIPT_HISTORY_ALL.CASH_RECEIPT_HISTORY_ID = AR_RECEIVABLE_APPLICATIONS_ALL.CASH_RECEIPT_HISTORY_ID
LEFT OUTER JOIN (select * from bec_ods.ra_batch_sources_all where is_deleted_flg <> 'Y')  ra_batch_sources_all
ON ra_customer_trx_all.batch_source_id = ra_batch_sources_all.batch_source_id
WHERE AR_DISTRIBUTIONS_ALL.SOURCE_TABLE = 'RA'
AND AR_DISTRIBUTIONS_ALL.SOURCE_TYPE IN ('REC', 'UNAPP', 'ACC', 'UNID', 'OTHER ACC')
AND AR_RECEIVABLE_APPLICATIONS_ALL.APPLICATION_TYPE = 'CASH'
AND NVL(AR_RECEIVABLE_APPLICATIONS_ALL.CONFIRMED_FLAG, 'Y') = 'Y'
/* Global Amounts */
)a
LEFT OUTER JOIN
 (select * from bec_ods.GL_DAILY_RATES
where is_deleted_flg<>'Y' and to_currency = 'USD' 
and conversion_type = 'Corporate' ) DCR
on a.RECEIPT_CURRENCY = DCR.from_currency
and DCR.conversion_date = a.GL_DATE 
group by 
receipt_app
,operating_unit
,receivable_application_id
,receivable_application_id_key
,gl_date
,set_of_books_id
,apply_date
,application_type
,RCV_STATUS
,RCV_AMOUNT_APPLIED
,display
,gl_posted_date
,org_id_key
,ORG_ID
,receipt_info
,CASH_RECEIPT_ID
,applied_customer_trx_id
,applied_payment_schedule_id
,receipt_currency
,pay_from_customer
,receipt_number
,receipt_date
,bank_account_id
,bank_acct_use_id
,receipt_cust_site_use_id
,cust_site_use_id_key
,receipt_method_id
,amount_due_original
,amount_due_remaining
,amount_applied
,schedule_id
,due_date
,payment_status
,receipt_legal_entity
,receipt_amount_f
,exchange_rate
,exchange_rate_type
,exchange_rate_date
,remit_bank_acct_use_id
,invoice_info
,invoice_id
,invoice_id_key
,invoice_number
,inv_trx_type_id
,inv_trx_type_id_key
,invoice_date
,inv_bill_to_contact_id
,inv_sold_to_site_use_id
,inv_bill_to_cust_id
,inv_bill_to_cust_id_key
,inv_bill_site_use_id
,inv_ship_site_use_id
,inv_bill_to_site_use_id_key
,inv_ship_to_site_use_id_key
,inv_term_id
,inv_term_id_key
,inv_sales_rep_id
,inv_sales_rep_id_key
,inv_po
,inv_amount
,inv_remaining_amount
,receipt_amt_applied_on_invoice
,amount_line_items_original
,amount_line_items_remaining
,inv_amt_adjusted
,inv_amt_dispute
,inv_amt_creditted
,inv_charges_amt
,inv_charges_remaining
,inv_freight_amt
,inv_freight_amt_remaining
,inv_tax_original
,inv_tax_remaining
,inv_discount_earned
,inv_discount_unearned
,inv_pay_schedule_id
,inv_due_date
,invoice_status
,proj_num
,inv_territory
,interface_header_attribute1
,interface_header_context
,inv_pay_site_use_id
,receipt_applied_distr_info
,DIST_LINE_ID
,RECEIPT_APP_DIST_LINE_ID_KEY,
RECEIPT_APP_CC_ID, 
RECEIPT_APP_AMT_DR,
RECEIPT_APP_AMT_CR,
RECEIPT_APP_AMT_ACCT_DR,
RECEIPT_APP_AMT_ACCT_CR,
SOURCE_ID,
RECEIPT_APPLIED_HISTORY_INFO,
RECEIPT_HISTORY_ID,
RECEIPT_HISTORY_ID_KEY,
amount
,acct_amount
,current_flag
,reverse_gl_date
,exchange_gain_loss
,source_name
,a.IS_DELETED_FLG
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
,cast(NVL(AMOUNT_DUE_ORIGINAL,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) 
,cast(NVL(AMOUNT_DUE_REMAINING,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) 
,cast(NVL(AMOUNT_APPLIED,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) 
,cast(NVL(INV_AMOUNT,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) 
,cast(NVL(INV_REMAINING_AMOUNT,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) 
,cast(NVL(INV_FREIGHT_AMT,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) 
,cast(NVL(INV_FREIGHT_AMT_REMAINING,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) 
,cast(NVL(ACCT_AMOUNT,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) 
,cast(NVL(AMOUNT,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) 
,cast(NVL(RCV_AMOUNT_APPLIED,0) * NVL(DCR.conversion_rate,1) as decimal(18,2))
,NVL(DCR.conversion_rate,1)
);


END;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ar_receipts'
	and batch_name = 'ar';

commit;