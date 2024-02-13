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
	bec_dwh.FACT_AR_XLA
where
	(nvl(ENTITY_ID,0),
	nvl(AE_HEADER_ID,0),
	nvl(AE_LINE_NUM,0),
	nvl(LINE_ID,0),
	nvl(CODE_COMBINATION_ID,0),
	nvl(SOURCE_DISTRIBUTION_ID_NUM_1,0)) in 
	(
	select
		nvl(ods.ENTITY_ID,0) as ENTITY_ID,
		nvl(ods.AE_HEADER_ID,0) as AE_HEADER_ID,
		nvl(ods.AE_LINE_NUM,0) as AE_LINE_NUM,
		nvl(ods.LINE_ID,0) as LINE_ID,
		nvl(ods.CODE_COMBINATION_ID,0) as CODE_COMBINATION_ID,
		nvl(ods.SOURCE_DISTRIBUTION_ID_NUM_1,0) as SOURCE_DISTRIBUTION_ID_NUM_1
	from
		bec_dwh.FACT_AR_XLA dw,
		(
		select
			XTE.ENTITY_ID as ENTITY_ID,
			XAH.AE_HEADER_ID as AE_HEADER_ID,
			XAL.AE_LINE_NUM as AE_LINE_NUM,
			ADA.LINE_ID as LINE_ID,
			XAL.CODE_COMBINATION_ID as CODE_COMBINATION_ID,
			XDL.SOURCE_DISTRIBUTION_ID_NUM_1 as SOURCE_DISTRIBUTION_ID_NUM_1,
			XAH.kca_seq_date
from
	bec_ods.XLA_TRANSACTION_ENTITIES XTE,
	bec_ods.XLA_AE_HEADERS XAH,
	bec_ods.XLA_AE_LINES XAL,
	bec_ods.XLA_DISTRIBUTION_LINKS XDL,
	bec_ods.ar_distributions_all ada
where
	XTE.ENTITY_ID = XAH.ENTITY_ID
	and XAH.AE_HEADER_ID = XAL.AE_HEADER_ID
	and XAH.AE_HEADER_ID = XDL.AE_HEADER_ID
	and XAL.ae_line_num = XDL.ae_line_num
	and XDL.SOURCE_DISTRIBUTION_ID_NUM_1 = ADA.source_id)ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS'
)|| '-' || nvl(ods.ENTITY_ID, 0)
|| '-' || nvl(ods.AE_HEADER_ID, 0)
|| '-' || nvl(ods.AE_LINE_NUM, 0)
|| '-' || nvl(ods.LINE_ID, 0)
|| '-' || nvl(ods.CODE_COMBINATION_ID, 0)
|| '-' || nvl(ods.SOURCE_DISTRIBUTION_ID_NUM_1, 0)
			and ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_ar_xla'
				and batch_name = 'ar')
);

commit;
-- Insert records

insert
	into
	bec_dwh.FACT_AR_XLA 
(
	xla_dist_type,
	entity_id,
	entity_id_key,
	application_id,
	application_id_key,
	legal_entity_id,
	legal_entity_id_key,
	entity_code,
	source_id_int_1,
	transaction_number,
	valuation_method,
	source_application_id,
	security_id_int_1,
	event_type_code,
	event_date,
	event_status_code,
	process_status_code,
	on_hold_flag,
	transaction_date,
	ae_header_id,
	ledger_id,
	ledger_id_key,
	header_accounting_date,
	gl_transfer_status_code,
	gl_transfer_date,
	je_category_name,
	accounting_entry_status_code,
	accounting_entry_type_code,
	header_description,
	budget_version_id,
	balance_type_code,
	period_name,
	ae_line_num,
	code_combination_id,
	gl_sl_link_id,
	accounting_class_code,
	entered_dr,
	entered_cr,
	accounted_dr,
	accounted_cr,
	sla_line_description,
	currency_code,
	gl_sl_link_table,
	line_unrounded_accounted_dr,
	line_unrounded_accounted_cr,
	line_unrounded_entered_dr,
	line_unrounded_entered_cr,
	line_accounting_date,
	source_table,
	source_distribution_type,
	source_distribution_id_num_1,
	event_class_code,
	dist_unrounded_entered_dr,
	dist_unrounded_entered_cr,
	dist_unrounded_accounted_dr,
	dist_unrounded_accounted_cr,
	applied_to_source_id_num_1,
	applied_to_distribution_type,
	applied_to_dist_id_num_1,
	source_id,
	line_id,
	line_id_key,
	org_id,
	org_id_key,
	amount_dr,
	amount_cr,
	acctd_amount_dr,
	acctd_amount_cr,
	customer_trx_id,
	customer_trx_id_key,
	customer_trx_line_id,
	customer_trx_line_id_key,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date,
	last_update_date)
(
	SELECT 
 XLA_DIST_TYPE,
 ENTITY_ID,
 (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||ENTITY_ID as ENTITY_ID_KEY,
 APPLICATION_ID,
 (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||APPLICATION_ID as APPLICATION_ID_KEY,
 LEGAL_ENTITY_ID,
 (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||LEGAL_ENTITY_ID as LEGAL_ENTITY_ID_KEY,
 ENTITY_CODE,
 SOURCE_ID_INT_1,
 TRANSACTION_NUMBER,
 VALUATION_METHOD,
 SOURCE_APPLICATION_ID,
 SECURITY_ID_INT_1,
 EVENT_TYPE_CODE,
 EVENT_DATE,
 EVENT_STATUS_CODE, 
 PROCESS_STATUS_CODE, 
 ON_HOLD_FLAG,
 TRANSACTION_DATE, 
 AE_HEADER_ID,
 LEDGER_ID,
 (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||LEDGER_ID as LEDGER_ID_KEY,
 HEADER_ACCOUNTING_DATE ,
 GL_TRANSFER_STATUS_CODE,
 GL_TRANSFER_DATE,
 JE_CATEGORY_NAME,
 ACCOUNTING_ENTRY_STATUS_CODE,
 ACCOUNTING_ENTRY_TYPE_CODE,
 HEADER_DESCRIPTION,
 BUDGET_VERSION_ID,
 BALANCE_TYPE_CODE,
 PERIOD_NAME,
 AE_LINE_NUM,
 CODE_COMBINATION_ID,
 GL_SL_LINK_ID,
 ACCOUNTING_CLASS_CODE,
 ENTERED_DR,
 ENTERED_CR,
 ACCOUNTED_DR,
 ACCOUNTED_CR,
 SLA_LINE_DESCRIPTION,
 CURRENCY_CODE,
 GL_SL_LINK_TABLE,
 LINE_UNROUNDED_ACCOUNTED_DR ,
 LINE_UNROUNDED_ACCOUNTED_CR ,
 LINE_UNROUNDED_ENTERED_DR,
 LINE_UNROUNDED_ENTERED_CR,
 LINE_ACCOUNTING_DATE,
 SOURCE_TABLE,
 SOURCE_DISTRIBUTION_TYPE,
 SOURCE_DISTRIBUTION_ID_NUM_1,
 EVENT_CLASS_CODE,
 DIST_UNROUNDED_ENTERED_DR,
 DIST_UNROUNDED_ENTERED_CR,
 DIST_UNROUNDED_ACCOUNTED_DR,
 DIST_UNROUNDED_ACCOUNTED_CR,
 APPLIED_TO_SOURCE_ID_NUM_1,
 APPLIED_TO_DISTRIBUTION_TYPE,
 APPLIED_TO_DIST_ID_NUM_1,
 source_id,
 line_id,
 (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||line_id as line_id_key,
 ORG_ID,
 (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||ORG_ID as ORG_ID_key,
 AMOUNT_DR,
 AMOUNT_CR,
 ACCTD_AMOUNT_DR,
 ACCTD_AMOUNT_CR,
 CUSTOMER_TRX_ID,
 (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||CUSTOMER_TRX_ID as CUSTOMER_TRX_ID_KEY,
 CUSTOMER_TRX_LINE_ID,
 (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||CUSTOMER_TRX_LINE_ID as CUSTOMER_TRX_LINE_ID_KEY,
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
       || nvl(ENTITY_ID, 0)
	   || '-'
       || nvl(AE_HEADER_ID, 0) 
	   || '-'
       || nvl(AE_LINE_NUM, 0) 
	   || '-'
       || nvl(LINE_ID, 0)
	   || '-'
       || nvl(CODE_COMBINATION_ID, 0)
	   || '-'
       || nvl(SOURCE_DISTRIBUTION_ID_NUM_1, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date,
LAST_UPDATE_DATE	
FROM
(
SELECT  
 'XLA_TE' XLA_DIST_TYPE ,
 XTE.ENTITY_ID,
 XTE.APPLICATION_ID,
 XTE.LEGAL_ENTITY_ID,
 XTE.ENTITY_CODE,
 XTE.SOURCE_ID_INT_1,
 XTE.TRANSACTION_NUMBER,
 XTE.VALUATION_METHOD,
 XTE.SOURCE_APPLICATION_ID,
 XTE.SECURITY_ID_INT_1,
 XE.EVENT_TYPE_CODE,
 XE.EVENT_DATE,
 XE.EVENT_STATUS_CODE, 
 XE.PROCESS_STATUS_CODE, 
 XE.ON_HOLD_FLAG,
 XE.TRANSACTION_DATE, 
 XAH.AE_HEADER_ID,
 XAH.LEDGER_ID,
 XAH.ACCOUNTING_DATE  HEADER_ACCOUNTING_DATE ,
 XAH.GL_TRANSFER_STATUS_CODE,
 XAH.GL_TRANSFER_DATE,
 XAH.JE_CATEGORY_NAME,
 XAH.ACCOUNTING_ENTRY_STATUS_CODE,
 XAH.ACCOUNTING_ENTRY_TYPE_CODE,
 XAH.DESCRIPTION HEADER_DESCRIPTION
 ,XAH.BUDGET_VERSION_ID,
 XAH.BALANCE_TYPE_CODE,
 XAH.PERIOD_NAME,
 XAL.AE_LINE_NUM,
 XAL.CODE_COMBINATION_ID,
 XAL.GL_SL_LINK_ID,
 XAL.ACCOUNTING_CLASS_CODE,
 XAL.ENTERED_DR,
 XAL.ENTERED_CR,
 XAL.ACCOUNTED_DR,
 XAL.ACCOUNTED_CR,
 XAL.DESCRIPTION SLA_LINE_DESCRIPTION,
 XAL.CURRENCY_CODE,
 XAL.GL_SL_LINK_TABLE,
 nvl(XAL.UNROUNDED_ACCOUNTED_DR,0) LINE_UNROUNDED_ACCOUNTED_DR ,
 nvl(XAL.UNROUNDED_ACCOUNTED_CR,0) LINE_UNROUNDED_ACCOUNTED_CR ,
 nvl(XAL.UNROUNDED_ENTERED_DR,0) LINE_UNROUNDED_ENTERED_DR,
 nvl(XAL.UNROUNDED_ENTERED_CR,0) LINE_UNROUNDED_ENTERED_CR,
 XAL.ACCOUNTING_DATE LINE_ACCOUNTING_DATE,
 XAL.SOURCE_TABLE,
 null SOURCE_DISTRIBUTION_TYPE,
 null SOURCE_DISTRIBUTION_ID_NUM_1,
 null EVENT_CLASS_CODE,
NULL DIST_UNROUNDED_ENTERED_DR,
NULL DIST_UNROUNDED_ENTERED_CR,
NULL DIST_UNROUNDED_ACCOUNTED_DR,
NULL DIST_UNROUNDED_ACCOUNTED_CR,
 null APPLIED_TO_SOURCE_ID_NUM_1,
 null APPLIED_TO_DISTRIBUTION_TYPE,
 null APPLIED_TO_DIST_ID_NUM_1,
 null source_id
,NULL line_id
,SECURITY_ID_INT_1 ORG_ID
,NULL AMOUNT_DR
,NULL AMOUNT_CR
,NULL ACCTD_AMOUNT_DR
,NULL ACCTD_AMOUNT_CR
,SOURCE_ID_INT_1 CUSTOMER_TRX_ID,
null CUSTOMER_TRX_LINE_ID,
XAH.LAST_UPDATE_DATE
from 
bec_ods.XLA_TRANSACTION_ENTITIES XTE,
bec_ods.XLA_AE_HEADERS XAH,
bec_ods.XLA_AE_LINES XAL,
bec_ods.XLA_EVENTS XE
where 1=1
AND XTE.APPLICATION_ID = 222 ---200
AND XTE.ENTITY_ID = XAH.ENTITY_ID  
and nvl(XE.event_id,0)=nvl(XAH.event_id,0)
AND XAH.AE_HEADER_ID = XAL.AE_HEADER_ID
UNION ALL
SELECT 
'XLA_ARD' XLA_DIST_TYPE,
NULL ENTITY_ID,
XDL.APPLICATION_ID,
null LEGAL_ENTITY_ID,
null ENTITY_CODE,
null SOURCE_ID_INT_1,
null TRANSACTION_NUMBER,
null VALUATION_METHOD,
null SOURCE_APPLICATION_ID,
null SECURITY_ID_INT_1,
XDL.EVENT_TYPE_CODE, 
null EVENT_DATE,
null EVENT_STATUS_CODE,
null PROCESS_STATUS_CODE,
null ON_HOLD_FLAG,
null TRANSACTION_DATE,
XAH.AE_HEADER_ID,
XAH.LEDGER_ID,
XAH.ACCOUNTING_DATE  HEADER_ACCOUNTING_DATE ,
XAH.GL_TRANSFER_STATUS_CODE,
XAH.GL_TRANSFER_DATE,
 XAH.JE_CATEGORY_NAME,
 XAH.ACCOUNTING_ENTRY_STATUS_CODE,
 XAH.ACCOUNTING_ENTRY_TYPE_CODE,
 XAH.DESCRIPTION HEADER_DESCRIPTION
 ,XAH.BUDGET_VERSION_ID,
 XAH.BALANCE_TYPE_CODE,
 XAH.PERIOD_NAME,
 XAL.AE_LINE_NUM,
 XAL.CODE_COMBINATION_ID,
 XAL.GL_SL_LINK_ID,
 XAL.ACCOUNTING_CLASS_CODE,
 XAL.ENTERED_DR,
 XAL.ENTERED_CR,
 XAL.ACCOUNTED_DR,
 XAL.ACCOUNTED_CR,
 XAL.DESCRIPTION SLA_LINE_DESCRIPTION,
 XAL.CURRENCY_CODE,
 XAL.GL_SL_LINK_TABLE,
 nvl(XAL.UNROUNDED_ACCOUNTED_DR,0) LINE_UNROUNDED_ACCOUNTED_DR ,
 nvl(XAL.UNROUNDED_ACCOUNTED_CR,0) LINE_UNROUNDED_ACCOUNTED_CR,
 nvl(XAL.UNROUNDED_ENTERED_DR,0) LINE_UNROUNDED_ENTERED_DR,
 nvl(XAL.UNROUNDED_ENTERED_CR,0) LINE_UNROUNDED_ENTERED_CR,
 XAL.ACCOUNTING_DATE LINE_ACCOUNTING_DATE,
 XAL.SOURCE_TABLE,
XDL.SOURCE_DISTRIBUTION_TYPE,
XDL.SOURCE_DISTRIBUTION_ID_NUM_1,
XDL.EVENT_CLASS_CODE, 
nvl(XDL.UNROUNDED_ENTERED_DR,0) DIST_UNROUNDED_ENTERED_DR,
nvl(XDL.UNROUNDED_ENTERED_CR,0) DIST_UNROUNDED_ENTERED_CR,
nvl(XDL.UNROUNDED_ACCOUNTED_CR,0) DIST_UNROUNDED_ACCOUNTED_DR,
nvl(XDL.UNROUNDED_ACCOUNTED_DR,0) DIST_UNROUNDED_ACCOUNTED_CR,
XDL.APPLIED_TO_SOURCE_ID_NUM_1,
XDL.APPLIED_TO_DISTRIBUTION_TYPE,
XDL.APPLIED_TO_DIST_ID_NUM_1,
ADA.source_id
,ADA.line_id
,ADA.org_id
,ADA.AMOUNT_DR
,ADA.AMOUNT_CR
,ADA.ACCTD_AMOUNT_DR
,ADA.ACCTD_AMOUNT_CR
,null CUSTOMER_TRX_ID,
null CUSTOMER_TRX_LINE_ID,
XAH.LAST_UPDATE_DATE
FROM 
bec_ods.XLA_AE_HEADERS XAH,
bec_ods.XLA_AE_LINES XAL,
bec_ods.XLA_DISTRIBUTION_LINKS XDL, 
bec_ods.ar_distributions_all ADA
WHERE 1=1
AND XDL.APPLICATION_ID =222
AND XAL.AE_HEADER_ID=XAH.AE_HEADER_ID 
AND XAH.AE_HEADER_ID=XDL.AE_HEADER_ID
and XAL.ae_line_num = XDL.ae_line_num --added join condition to handle duplicates
and XDL.SOURCE_DISTRIBUTION_ID_NUM_1=ADA.source_id
AND XDL.SOURCE_DISTRIBUTION_TYPE = 'AR_DISTRIBUTIONS_ALL'
UNION ALL 
select 
'XLA_DL' XLA_DIST_TYPE,
NULL ENTITY_ID,
XDL.APPLICATION_ID,
null LEGAL_ENTITY_ID,
null ENTITY_CODE,
null SOURCE_ID_INT_1,
null TRANSACTION_NUMBER,
null VALUATION_METHOD,
null SOURCE_APPLICATION_ID,
null SECURITY_ID_INT_1,
XDL.EVENT_TYPE_CODE, 
null EVENT_DATE,
null EVENT_STATUS_CODE,
null PROCESS_STATUS_CODE,
null ON_HOLD_FLAG,
null TRANSACTION_DATE,
XAH.AE_HEADER_ID,
XAH.LEDGER_ID,
XAH.ACCOUNTING_DATE  HEADER_ACCOUNTING_DATE ,
XAH.GL_TRANSFER_STATUS_CODE,
XAH.GL_TRANSFER_DATE,
 XAH.JE_CATEGORY_NAME,
 XAH.ACCOUNTING_ENTRY_STATUS_CODE,
 XAH.ACCOUNTING_ENTRY_TYPE_CODE,
 XAH.DESCRIPTION HEADER_DESCRIPTION
 ,XAH.BUDGET_VERSION_ID,
 XAH.BALANCE_TYPE_CODE,
 XAH.PERIOD_NAME,
 XAL.AE_LINE_NUM,
 XAL.CODE_COMBINATION_ID,
 XAL.GL_SL_LINK_ID,
 XAL.ACCOUNTING_CLASS_CODE,
 XAL.ENTERED_DR,
 XAL.ENTERED_CR,
 XAL.ACCOUNTED_DR,
 XAL.ACCOUNTED_CR,
 XAL.DESCRIPTION SLA_LINE_DESCRIPTION,
 XAL.CURRENCY_CODE,
 XAL.GL_SL_LINK_TABLE,
 nvl(XAL.UNROUNDED_ACCOUNTED_DR,0) LINE_UNROUNDED_ACCOUNTED_DR ,
 nvl(XAL.UNROUNDED_ACCOUNTED_CR,0) LINE_UNROUNDED_ACCOUNTED_CR,
 nvl(XAL.UNROUNDED_ENTERED_DR,0) LINE_UNROUNDED_ENTERED_DR,
 nvl(XAL.UNROUNDED_ENTERED_CR,0) LINE_UNROUNDED_ENTERED_CR,
 XAL.ACCOUNTING_DATE LINE_ACCOUNTING_DATE,
 XAL.SOURCE_TABLE,
XDL.SOURCE_DISTRIBUTION_TYPE,
XDL.SOURCE_DISTRIBUTION_ID_NUM_1,
XDL.EVENT_CLASS_CODE, 
nvl(XDL.UNROUNDED_ENTERED_DR,0) DIST_UNROUNDED_ENTERED_DR,
nvl(XDL.UNROUNDED_ENTERED_CR,0) DIST_UNROUNDED_ENTERED_CR,
nvl(XDL.UNROUNDED_ACCOUNTED_CR,0) DIST_UNROUNDED_ACCOUNTED_DR,
nvl(XDL.UNROUNDED_ACCOUNTED_DR,0) DIST_UNROUNDED_ACCOUNTED_CR,
XDL.APPLIED_TO_SOURCE_ID_NUM_1,
XDL.APPLIED_TO_DISTRIBUTION_TYPE,
XDL.APPLIED_TO_DIST_ID_NUM_1,
NULL source_id,
NULL line_id,
RTGDA.org_id
,NULL AMOUNT_DR
,NULL AMOUNT_CR
,NULL ACCTD_AMOUNT_DR
,NULL ACCTD_AMOUNT_CR
,RTGDA.CUSTOMER_TRX_ID
,RTGDA.CUSTOMER_TRX_LINE_ID
,XAH.LAST_UPDATE_DATE
FROM 
bec_ods.XLA_AE_HEADERS XAH,
bec_ods.XLA_AE_LINES XAL,
bec_ods.XLA_DISTRIBUTION_LINKS XDL,
bec_ods.RA_CUST_TRX_LINE_GL_DIST_ALL  RTGDA
WHERE 1=1
AND XDL.APPLICATION_ID =222
AND XAL.AE_HEADER_ID=XAH.AE_HEADER_ID 
AND XAH.AE_HEADER_ID=XDL.AE_HEADER_ID
and XAL.ae_line_num = XDL.ae_line_num --added join condition to handle duplicates
and XDL.SOURCE_DISTRIBUTION_ID_NUM_1=RTGDA.CUST_TRX_LINE_GL_DIST_ID
AND XDL.SOURCE_DISTRIBUTION_TYPE = 'RA_CUST_TRX_LINE_GL_DIST_ALL'
and XAH.kca_seq_date > (
				select
					(executebegints-prune_days)
				from
					bec_etl_ctrl.batch_dw_info
				where
					dw_table_name = 'fact_ar_xla'
					and batch_name = 'ar')
)					
);
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_ar_xla'
	and batch_name = 'ar';

COMMIT;