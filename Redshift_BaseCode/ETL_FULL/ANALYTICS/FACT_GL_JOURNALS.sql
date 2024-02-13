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

drop table if exists bec_dwh.FACT_GL_JOURNALS;

create table bec_dwh.FACT_GL_JOURNALS diststyle all sortkey(LEDGER_ID,JE_HEADER_ID,JE_LINE_NUM,JE_BATCH_ID,CODE_COMBINATION_ID)
as 
(
select
 JL.JE_HEADER_ID
,JL.JE_LINE_NUM
,JL.LEDGER_ID
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||JL.LEDGER_ID as LEDGER_ID_KEY
,GL.PERIOD_SET_NAME
,GCC.CHART_OF_ACCOUNTS_ID
,JL.PERIOD_NAME "PERIOD_NAME"
,GCC.CODE_COMBINATION_ID
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||GCC.CODE_COMBINATION_ID as CODE_COMBINATION_ID_KEY
,GCC.ACCOUNT_TYPE 
,gcc.segment1  company
,gcc.segment2  department
,gcc.segment3  Account
,gcc.segment5  budget_id
,gcc.segment4  intercompany
,gcc.segment6  "LOCATION"
,JL.STATUS "JOURNAL_LINE_STATUS"
,JH.STATUS "JOURNAL_HEADER_STATUS"
,JB.STATUS "JOURNAL_BATCH_STATUS" 
,JL.EFFECTIVE_DATE
,JL.DESCRIPTION as LINE_DESCRIPTION
,JH.DESCRIPTION as HEADER_DESCRIPTION
,JH.JE_CATEGORY
,JH.JE_SOURCE "JOURNAL_SOURCE"
,JH.NAME "JOURNAL_NAME"
,JH.ACTUAL_FLAG
,JB.NAME "BATCH_NAME" 
,jb.description batch_description
,jb.posted_by
,JB.ORG_ID as ORG_ID
,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||JB.ORG_ID as ORG_ID_KEY
,JH.JE_BATCH_ID
,JH.POSTED_DATE "POSTED_DATE"
,JL.LAST_UPDATED_BY 
,JL.CREATED_BY
,JL.LAST_UPDATE_DATE "LINE_LAST_UPDATE"
,JL.CREATION_DATE "LINE_CREATION_DATE"
,JH.DATE_CREATED "DATE_CREATED"
,JH.JE_FROM_SLA_FLAG
,JH.TAX_STATUS_CODE
,JH.CURRENCY_CONVERSION_RATE
,JH.CURRENCY_CODE
,jh.external_reference
,jh.running_total_accounted_cr
,jh.running_total_accounted_dr
,JL.ENTERED_DR
,JL.ENTERED_CR
,JL.ACCOUNTED_DR
,JL.ACCOUNTED_CR 
,JL.REFERENCE_1
,JL.REFERENCE_2
,JL.REFERENCE_3
,JL.REFERENCE_4
,JL.REFERENCE_5
,JL.REFERENCE_6
,JL.REFERENCE_7
,JL.REFERENCE_8
,JL.REFERENCE_9
,JL.REFERENCE_10
,JL.GL_SL_LINK_ID
,JL.GL_SL_LINK_TABLE
,JL.ATTRIBUTE1 project_number
,JL.ATTRIBUTE2 task_number
,JL.ATTRIBUTE3 expnd_type
,JL.PERIOD_NAME||JH.NAME||JL.JE_LINE_NUM as orig_transaction_reference
,'N' AS IS_DELETED_FLG
,(select
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
		source_system = 'EBS')
	   || '-' || nvl(GL.LEDGER_ID, 0) 
	   || '-' || nvl(JL.JE_HEADER_ID, 0) 
	   || '-' || nvl(JL.JE_LINE_NUM, 0) 
	   || '-' || nvl(JB.JE_BATCH_ID, 0) 
	   || '-' || nvl(GCC.CODE_COMBINATION_ID, 0)
	   as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(select * from bec_ods.GL_JE_LINES where is_deleted_flg <> 'Y') JL 
,(select * from bec_ods.GL_JE_HEADERS where is_deleted_flg <> 'Y') JH 
,(select * from bec_ods.GL_JE_BATCHES where is_deleted_flg <> 'Y') JB 
,(select * from bec_ods.GL_CODE_COMBINATIONS where is_deleted_flg <> 'Y') GCC 
,(select * from bec_ods.GL_LEDGERS where is_deleted_flg <> 'Y') GL
where
	1 = 1
	and GCC.CODE_COMBINATION_ID = JL.CODE_COMBINATION_ID
	and JH.JE_HEADER_ID = JL.JE_HEADER_ID
	and JH.JE_BATCH_ID = JB.JE_BATCH_ID
	and JL.LEDGER_ID = GL.LEDGER_ID
	and GCC.SUMMARY_FLAG = 'N'
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_gl_journals'
	and batch_name = 'gl';

commit;