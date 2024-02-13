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

drop table if exists bec_dwh.FACT_AP_AGING;

create table bec_dwh.fact_ap_aging 
	diststyle all
	sortkey (INVOICE_ID,PAYMENT_NUM)
as
(
select
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||APSC.INVOICE_ID as INVOICE_ID_KEY,
	API.INVOICE_NUM,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||API.VENDOR_ID as VENDOR_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||APSC.ORG_ID as ORG_ID_KEY,
	APSC.ORG_ID as ORG_ID,
	APSC.payment_num,
	API.INVOICE_CURRENCY_CODE,
	API.PAYMENT_CURRENCY_CODE,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||API.VENDOR_SITE_ID as VENDOR_SITE_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||API.SET_OF_BOOKS_ID as LEDGER_ID_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||API.TERMS_ID as TERMS_ID_KEY,
	APSC.INVOICE_ID,
	API.INVOICE_DATE,
	APSC.DUE_DATE,
	API.INVOICE_AMOUNT,
    API.AMOUNT_PAID,
    APSC.AMOUNT_REMAINING,
    case when api.payment_status_flag = 'P' 
              and APSC.payment_status_flag = 'P'
              and API.INVOICE_AMOUNT = API.AMOUNT_PAID
              then APSC.AMOUNT_REMAINING
              else 0
              end             prepay_amount,
    api.payment_status_flag inv_payment_status_flag,
    APSC.payment_status_flag payment_status_flag,
    NVL(API.BASE_AMOUNT, API.INVOICE_AMOUNT) "INVOICE_BASE_AMOUNT",
	'ABC' "HOLD_FLAG",
	(TRUNC(SYSDATE) - TRUNC(APSC.DUE_DATE)) "DAYS_OVERDUE",
	(case
		when TRUNC(SYSDATE) - TRUNC(APSC.DUE_DATE) <= 0
                   then SUM (APSC.AMOUNT_REMAINING)
		else 0
	end
            ) "CURRENT_AP",
	(case
		when TRUNC(SYSDATE) - TRUNC(APSC.DUE_DATE) between 1 and 15
                   then SUM (APSC.AMOUNT_REMAINING)
		else 0
	end
            ) "AP01_15",
	(case
		when TRUNC(SYSDATE) - TRUNC(APSC.DUE_DATE) between 16 and 30
                   then SUM (APSC.AMOUNT_REMAINING)
		else 0
	end
            ) "AP16_30",
	(case
		when TRUNC(SYSDATE) - TRUNC(APSC.DUE_DATE) between 31 and 60
                   then SUM (APSC.AMOUNT_REMAINING)
		else 0
	end
            ) "AP31_60",
	(case
		when TRUNC(SYSDATE) - TRUNC(APSC.DUE_DATE) between 61 and 90
                   then SUM (APSC.AMOUNT_REMAINING)
		else 0
	end
            ) "AP61_90",
	(case
		when TRUNC(SYSDATE) - TRUNC(APSC.DUE_DATE) > 90
                   then SUM (APSC.AMOUNT_REMAINING)
		else 0
	end
            ) "AP90_PLUS",
	cast(NVL(API.invoice_amount,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INVOICE_AMOUNT,
    cast(NVL(API.amount_paid,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT_PAID,
		cast(NVL(APSC.AMOUNT_REMAINING,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT_REMAINING,
	cast(NVL( 
    case when api.payment_status_flag = 'P' 
              and APSC.payment_status_flag = 'P'
              and API.INVOICE_AMOUNT = API.AMOUNT_PAID
              then APSC.AMOUNT_REMAINING
              else 0
              end  ,0)        * NVL(DCR.conversion_rate,1)as decimal(18,2))   GBL_PREPAY_AMOUNT,
   'N' AS IS_DELETED_FLG,		
			(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || nvl(APSC.INVOICE_ID, 0)|| '-' || nvl(APSC.PAYMENT_NUM, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(select * from bec_ods.AP_PAYMENT_SCHEDULES_ALL where is_deleted_flg <> 'Y') APSC,
	(select * from bec_ods.AP_INVOICES_ALL where is_deleted_flg <> 'Y') API,
    (select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y') DCR
where 1=1
    and	APSC.INVOICE_ID = API.INVOICE_ID
	and APSC.AMOUNT_REMAINING != 0
	and DCR.to_currency(+) = 'USD'
	and DCR.conversion_type(+) = 'Corporate'
	and API.invoice_currency_code = DCR.from_currency(+)
	and DCR.conversion_date(+) = API.invoice_date
	and case when  api.payment_status_flag = 'Y'
             and APSC.payment_status_flag = 'N'
             and invoice_amount = amount_paid  then 
             'N'
             else 
             'Y'
             end = 'Y'
group by
    INVOICE_ID_KEY,	
	API.INVOICE_NUM,
	VENDOR_ID_KEY,
	ORG_ID_KEY,
	APSC.ORG_ID,
	apsc.payment_num,								
	API.INVOICE_CURRENCY_CODE,
	API.PAYMENT_CURRENCY_CODE,
	VENDOR_SITE_ID_KEY,
	LEDGER_ID_KEY,
	TERMS_ID_KEY,
	APSC.INVOICE_ID,
	API.INVOICE_DATE,
	APSC.DUE_DATE,
	API.INVOICE_AMOUNT,
    API.AMOUNT_PAID,
    NVL(API.BASE_AMOUNT, API.INVOICE_AMOUNT),
	TRUNC(SYSDATE) - TRUNC(APSC.DUE_DATE),
	GBL_INVOICE_AMOUNT,
	GBL_AMOUNT_PAID,
	APSC.AMOUNT_REMAINING,
	api.payment_status_flag,
	apsc.payment_status_flag,
	NVL(DCR.conversion_rate,1)
);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ap_aging'
	and batch_name = 'ap';

commit;