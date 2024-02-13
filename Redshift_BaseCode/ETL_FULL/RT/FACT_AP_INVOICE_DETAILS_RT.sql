/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh_rpt.FACT_AP_INVOICE_DETAILS_RT;

create table bec_dwh_rpt.FACT_AP_INVOICE_DETAILS_RT
	distkey(org_id)
	sortkey(invoice_date)
	as (
	WITH AP_CHECKS AS (SELECT app.invoice_id, app.creation_date payment_date, app.payment_num, app.amount payment_amount,
       app.posted_flag ,apc.check_number, apc.check_date
  FROM bec_ods.ap_invoice_payments_all app, bec_ods.ap_checks_all apc
 WHERE app.check_id = apc.check_id
 --and app.org_id = 85
 )
 SELECT DISTINCT api.vendor_id,api.vendor_site_id,api.invoice_id ap_invoice_id, gcc.segment2 invoice_department,
                    gcc.segment3 invoice_account, api.invoice_num,
                    api.invoice_date, ail.line_number,
                    ail.amount invoice_line_amount,
                    aid.amount distribution_amount,
                    api.invoice_amount invoice_total_amount,
                    api.invoice_currency_code, api.amount_paid,
                    ail.project_id, ail.task_id, api.org_id,
	                aca.payment_date, 
					aca.payment_num, aca.payment_amount,
                    aca.check_number, aca.check_date,aca.posted_flag
					,api.DISCOUNT_AMOUNT_TAKEN
					,api.TERMS_DATE
					,api.FREIGHT_AMOUNT
					,api.INVOICE_RECEIVED_DATE
					,api.EXCHANGE_RATE,
                     api.EXCHANGE_DATE,
                     api.CANCELLED_DATE,
                     api.CREATION_DATE,
                     api.LAST_UPDATE_DATE,
                     api.CANCELLED_AMOUNT,
					 api.PAY_CURR_INVOICE_AMOUNT,
                     api.GL_DATE,
                     api.TOTAL_TAX_AMOUNT,
                     api.LEGAL_ENTITY_ID,
                     api.PARTY_ID,
                     api.PARTY_SITE_ID,
                     api.AMOUNT_APPLICABLE_TO_DISCOUNT,
					 NVL(api.BASE_AMOUNT, api.INVOICE_AMOUNT) "INVOICE_BASE_AMOUNT",
                     api.ORIGINAL_PREPAYMENT_AMOUNT,
					 cast(NVL(api.invoice_amount,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INVOICE_AMOUNT,
                     cast(NVL(api.amount_paid,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_AMOUNT_PAID,
					 api.INVOICE_TYPE_LOOKUP_CODE,
					 aid.POSTED_FLAG as inv_posted_flag,
					 AID.ACCOUNTING_DATE,
					 AID.DISTRIBUTION_LINE_NUMBER,
					 AID.LINE_TYPE_LOOKUP_CODE,
					 AID.DESCRIPTION,
					 api.SET_OF_BOOKS_ID as ledger_id
  FROM bec_ods.ap_invoice_lines_all ail,
       bec_ods.ap_invoices_all api,
       bec_dwh.FACT_AP_INV_DISTRIBUTIONS aid,
       bec_Dwh.dim_gl_accounts gcc
	  ,bec_ods.GL_DAILY_RATES DCR
      ,AP_CHECKS ACA	  
  WHERE ail.org_id = api.org_id
  AND ail.invoice_id = api.invoice_id
  AND ail.org_id = aid.org_id
  AND ail.invoice_id = aid.invoice_id
  AND ail.line_number = aid.invoice_line_number
  AND aid.GL_ACCOUNT_ID_KEY = gcc.dw_load_id
  --and api.org_id = 85
  AND ail.invoice_id = aca.invoice_id(+)
  and DCR.to_currency(+) = 'USD'
and DCR.conversion_type(+) = 'Corporate'
and api.invoice_currency_code = DCR.from_currency(+)
and DCR.conversion_date(+) = api.invoice_date
 ) ;
 
 end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ap_invoice_details_rt'
	and batch_name = 'ap';

commit;