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

drop table if exists bec_dwh.FACT_AR_INVOICE_BILLING;

create table bec_dwh.FACT_AR_INVOICE_BILLING diststyle all
sortkey (CUSTOMER_TRX_ID) 
as (SELECT   
    RAG.CUST_TRX_LINE_GL_DIST_ID,
    RAG.ORG_ID,
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RAG.ORG_ID as ORG_ID_KEY,
    RAG.CODE_COMBINATION_ID,
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RAG.CODE_COMBINATION_ID 
    as ACCOUNT_ID_KEY,
    RAG.CUSTOMER_TRX_ID,
    RAG.CUSTOMER_TRX_LINE_ID,
    APS.PAYMENT_SCHEDULE_ID,
    ARA.ADJUSTMENT_ID,
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||ARA.ADJUSTMENT_ID as ADJUSTMENT_ID_KEY,
    RAT.BATCH_SOURCE_ID,
    RAT.INVOICE_CURRENCY_CODE,
    RAT.CUST_TRX_TYPE_ID,
    RAT.TRX_DATE,
    RAS.NAME SOURCE_NAME,
    RAG.GL_DATE,
    APS.DUE_DATE,
    APS.AMOUNT_DUE_REMAINING,
    RAG.AMOUNT,
    NVL(ARA.AMOUNT,0) ADJ_AMOUNT,
		cast(NVL(APS.AMOUNT_DUE_REMAINING,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_INVOICE_AMOUNT,
    cast(NVL(RAG.AMOUNT,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT_PAID,	
	cast(NVL(ARA.AMOUNT,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_ADJ_AMOUNT,	
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
    || '-' || nvl(RAG.CUST_TRX_LINE_GL_DIST_ID, 0) 
    || '-' || nvl(ARA.ADJUSTMENT_ID, 0) 
	|| '-' || nvl(APS.PAYMENT_SCHEDULE_ID,0)
	as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
from 
(select * from bec_ods.RA_CUST_TRX_LINE_GL_DIST_ALL where is_deleted_flg <> 'Y')   RAG
inner join (select * from bec_ods.RA_CUSTOMER_TRX_LINES_ALL where is_deleted_flg <> 'Y')      RAL
on RAG.CUSTOMER_TRX_LINE_ID = RAL.CUSTOMER_TRX_LINE_ID
inner join (select * from bec_ods.RA_CUSTOMER_TRX_ALL where is_deleted_flg <> 'Y')            RAT
on RAG.CUSTOMER_TRX_ID = RAT.CUSTOMER_TRX_ID
left outer join (select * from bec_ods.AR_PAYMENT_SCHEDULES_ALL where is_deleted_flg <> 'Y')       APS
on RAT.CUSTOMER_TRX_ID = APS.CUSTOMER_TRX_ID
left outer join (select * from bec_ods.AR_ADJUSTMENTS_ALL where is_deleted_flg <> 'Y')             ARA
on RAT.CUSTOMER_TRX_ID = ARA.CUSTOMER_TRX_ID
left outer join (select * from bec_ods.RA_BATCH_SOURCES_ALL where is_deleted_flg <> 'Y')           RAS
on RAT.BATCH_SOURCE_ID = RAS.BATCH_SOURCE_ID
    AND RAG.ORG_ID = RAS.ORG_ID	
left outer join 
( select * from bec_ods.GL_DAILY_RATES  
where conversion_type = 'Corporate' and to_currency = 'USD'
and is_deleted_flg <> 'Y')DCR
on  DCR.from_currency = RAT.invoice_currency_code
and DCR.conversion_date= RAG.GL_DATE
);

END;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ar_invoice_billing'
	and batch_name = 'ar';

commit;