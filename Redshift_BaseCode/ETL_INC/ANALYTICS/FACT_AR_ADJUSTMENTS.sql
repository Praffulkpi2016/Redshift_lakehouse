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
	bec_dwh.FACT_AR_ADJUSTMENTS
where
	ADJUSTMENT_ID in (
	select
		ods.ADJUSTMENT_ID
	from
		bec_dwh.FACT_AR_ADJUSTMENTS dw,
		bec_ods.ar_adjustments_all ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS'
)|| '-' || nvl(ods.ADJUSTMENT_ID, 0)
			and (ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ar_adjustments'
				and batch_name = 'ar')
				or ods.is_deleted_flg = 'Y')
);

commit;
-- Insert records

insert
	into
	bec_dwh.FACT_AR_ADJUSTMENTS 
(
adjusment_info
,adjustment_id
,adjustment_number
,dist_source_id
,dist_source_table
,dist_source_type
,adj_status
,adj_org_id
,org_id_key
,ORG_ID
,adj_apply_date
,adj_gl_date
,adj_set_of_books_id
,ledger_id_key
,adj_type
,adj_adjust_type
,adj_gl_post_date
,adj_amount
,adj_line_adjusted
,adj_freight_adjusted
,adj_tax_adjusted
,adj_rec_charge_adjusted
,invoice_info
,inv_trx_id
,inv_trx_type_id_key
,inv_trx_number
,inv_trx_date
,inv_sold_to_site_use_id
,inv_bill_to_cust_id_key
,inv_bill_site_use_id_key
,inv_bill_to_contact_id
,inv_ship_site_use_id_key
,inv_term_id_key
,inv_sales_rep_id_key
,primary_salesrep_id
,term_id
,ship_to_site_use_id
,bill_to_site_use_id
,bill_to_customer_id
,cust_trx_type_id
,inv_po
,inv_territory
,inv_currency_code
,receipt_method_id_key
,receipt_method_id
,interface_header_attribute1
,interface_header_context
,inv_pay_site_use_id_key
,paying_site_use_id
,inv_legal_entity_id
,proj_num
,inv_payment_schdule_id
,inv_due_date
,inv_cust_site_id_key
,inv_cust_site_use_id_key
,customer_site_use_id
,terms_sequence_number
,inv_status
,adjustment_dist_info
,line_id
,amount_dr
,amount_cr
,acctd_amount_dr
,acctd_amount_cr
,last_update_date
,code_combination_id_key
,code_combination_id
 ,GBL_AMOUNT,
 GBL_ADJ_LINE_ADJUSTED,
 GBL_ADJ_FREIGHT_ADJUSTED,
 GBL_ADJ_REC_CHARGE_ADJUSTED,
 GBL_ADJ_TAX_ADJUSTED
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
	)
(	
select
'ADJUSTMENT_INFO'      Adjusment_Info,
AAA.ADJUSTMENT_ID      ADJUSTMENT_ID, 
AAA.ADJUSTMENT_NUMBER  ADJUSTMENT_NUMBER, 
ADAL.SOURCE_ID DIST_SOURCE_ID,
ADAL.SOURCE_TABLE DIST_SOURCE_TABLE,
ADAL.SOURCE_TYPE DIST_SOURCE_TYPE,
AAA.STATUS ADJ_STATUS,
AAA.ORG_ID ADJ_ORG_ID,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AAA.ORG_ID as ORG_ID_KEY,
AAA.ORG_ID as ORG_ID,
AAA.APPLY_DATE         ADJ_APPLY_DATE, 
AAA.GL_DATE            ADJ_GL_DATE, 
AAA.SET_OF_BOOKS_ID ADJ_SET_OF_BOOKS_ID,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||AAA.SET_OF_BOOKS_ID  as  LEDGER_ID_KEY, 
AAA.TYPE              ADJ_TYPE, 
AAA.ADJUSTMENT_TYPE    ADJ_ADJUST_TYPE, 
AAA.GL_POSTED_DATE     ADJ_GL_POST_DATE, 
AAA.AMOUNT ADJ_AMOUNT,
AAA.LINE_ADJUSTED ADJ_LINE_ADJUSTED,
AAA.FREIGHT_ADJUSTED ADJ_FREIGHT_ADJUSTED,
AAA.TAX_ADJUSTED ADJ_TAX_ADJUSTED,
AAA.RECEIVABLES_CHARGES_ADJUSTED ADJ_REC_CHARGE_ADJUSTED,
'INVOICE_INFO'     Invoice_Info,
RCTA.CUSTOMER_TRX_ID               INV_TRX_ID, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RCTA.CUST_TRX_TYPE_ID        INV_TRX_TYPE_ID_KEY, 
RCTA.TRX_NUMBER                    INV_TRX_NUMBER, 
RCTA.TRX_DATE                      INV_TRX_DATE, 
RCTA.SOLD_TO_SITE_USE_ID           Inv_Sold_to_Site_Use_Id, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||RCTA.BILL_TO_CUSTOMER_ID           INV_BILL_TO_CUST_ID_KEY, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-'||RCTA.BILL_TO_SITE_USE_ID           Inv_Bill_Site_Use_Id_KEY, 
RCTA.BILL_TO_CONTACT_ID            Inv_Bill_To_Contact_Id, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-'||RCTA.SHIP_TO_SITE_USE_ID           Inv_Ship_Site_Use_Id_KEY, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RCTA.TERM_ID     as      INV_TERM_ID_KEY, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RCTA.PRIMARY_SALESREP_ID   as   INV_SALES_REP_ID_KEY, 
RCTA.PRIMARY_SALESREP_ID, 
RCTA.TERM_ID,
RCTA.SHIP_TO_SITE_USE_ID,
RCTA.BILL_TO_SITE_USE_ID,
RCTA.BILL_TO_CUSTOMER_ID,
RCTA.CUST_TRX_TYPE_ID,
RCTA.PURCHASE_ORDER                Inv_PO, 
RCTA.TERRITORY_ID                  Inv_Territory, 
RCTA.INVOICE_CURRENCY_CODE         Inv_Currency_Code, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RCTA.RECEIPT_METHOD_ID as RECEIPT_METHOD_ID_KEY, 
RCTA.RECEIPT_METHOD_ID,
RCTA.INTERFACE_HEADER_ATTRIBUTE1, 
RCTA.INTERFACE_HEADER_CONTEXT, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RCTA.PAYING_SITE_USE_ID  as      INV_PAY_SITE_USE_ID_KEY,
RCTA.PAYING_SITE_USE_ID ,
RCTA.LEGAL_ENTITY_ID               Inv_Legal_Entity_Id,
RCTA.INTERFACE_HEADER_ATTRIBUTE1 AS PROJ_NUM,
APSA.PAYMENT_SCHEDULE_ID      Inv_Payment_Schdule_Id, 
APSA.DUE_DATE                 Inv_Due_Date, 
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||(select CUST_ACCT_SITE_ID from bec_ods.HZ_CUST_SITE_USES_ALL where is_deleted_flg <> 'Y' and site_use_id = APSA.CUSTOMER_SITE_USE_ID) INV_CUST_SITE_ID_KEY,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-'|| APSA.CUSTOMER_SITE_USE_ID as   INV_CUST_SITE_USE_ID_KEY, 
APSA.CUSTOMER_SITE_USE_ID,
APSA.TERMS_SEQUENCE_NUMBER, 
APSA.STATUS                   Inv_Status, 
'ADJUSTMENT_DIST_INFO'  Adjustment_Dist_Info,
ADAL.LINE_ID,
ADAL.AMOUNT_DR,
ADAL.AMOUNT_CR,
ADAL.ACCTD_AMOUNT_DR,
ADAL.ACCTD_AMOUNT_CR,
AAA.LAST_UPDATE_DATE,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||ADAL.CODE_COMBINATION_ID as CODE_COMBINATION_ID_KEY,
ADAL.CODE_COMBINATION_ID,
cast(NVL(AAA.AMOUNT,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT,
cast(NVL(AAA.LINE_ADJUSTED,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_ADJ_LINE_ADJUSTED,
cast(NVL(AAA.FREIGHT_ADJUSTED,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_ADJ_FREIGHT_ADJUSTED,
cast(NVL(AAA.RECEIVABLES_CHARGES_ADJUSTED,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_ADJ_REC_CHARGE_ADJUSTED,
cast(NVL(AAA.TAX_ADJUSTED,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_ADJ_TAX_ADJUSTED,
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
       || nvl(AAA.ADJUSTMENT_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM  (select * from bec_ods.ar_adjustments_all where is_deleted_flg <> 'Y') AAA  
INNER JOIN (select * from bec_ods.AR_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y') ADAL ON ADAL.SOURCE_ID= AAA.ADJUSTMENT_ID
INNER JOIN (select * from bec_ods.RA_CUSTOMER_TRX_ALL where is_deleted_flg <> 'Y')  RCTA ON RCTA.CUSTOMER_TRX_ID = AAA.CUSTOMER_TRX_ID 
INNER JOIN (select * from bec_ods.AR_PAYMENT_SCHEDULES_ALL where is_deleted_flg <> 'Y') APSA ON APSA.PAYMENT_SCHEDULE_ID = AAA.PAYMENT_SCHEDULE_ID  
LEFT OUTER JOIN  
(select * from bec_ods.GL_DAILY_RATES 
where conversion_type = 'Corporate' and to_currency = 'USD' and is_deleted_flg <> 'Y')DCR
on  DCR.from_currency = RCTA.invoice_currency_code
and DCR.conversion_date= AAA.GL_DATE
WHERE  ADAL.SOURCE_TABLE='ADJ' 
AND ADAL.SOURCE_TYPE='REC'
AND AAA.STATUS='A'
				and (AAA.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ar_adjustments'
				and batch_name = 'ar'))
);

commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_ar_adjustments'
	and batch_name = 'ar';

COMMIT;