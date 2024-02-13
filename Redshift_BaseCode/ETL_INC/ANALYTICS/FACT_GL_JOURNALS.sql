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

delete from bec_dwh.fact_gl_journals
where exists 
(select 1 
from
bec_ods.GL_JE_LINES   JL 
,bec_ods.GL_JE_HEADERS   JH 
,bec_ods.GL_JE_BATCHES   JB 
,bec_ods.GL_CODE_COMBINATIONS   GCC 
,bec_ods.GL_LEDGERS   GL
where
1 = 1
and GCC.CODE_COMBINATION_ID = JL.CODE_COMBINATION_ID
and JH.JE_HEADER_ID = JL.JE_HEADER_ID
and JH.JE_BATCH_ID = JB.JE_BATCH_ID
and JL.LEDGER_ID = GL.LEDGER_ID
and GCC.SUMMARY_FLAG = 'N'
and (
JL.kca_seq_date > (
select
(executebegints-prune_days)
from
bec_etl_ctrl.batch_dw_info
where
dw_table_name = 'fact_gl_journals'
and batch_name = 'gl')
or 
GCC.kca_seq_date > (
select
(executebegints-prune_days)
from
bec_etl_ctrl.batch_dw_info
where
dw_table_name = 'fact_gl_journals'
and batch_name = 'gl'))
and fact_gl_journals.JE_HEADER_ID = JL.JE_HEADER_ID
and fact_gl_journals.JE_LINE_NUM = JL.JE_LINE_NUM
and fact_gl_journals.CODE_COMBINATION_ID = GCC.CODE_COMBINATION_ID
);
commit;
-- Insert records
insert
into
bec_dwh.fact_gl_journals  
(
JE_HEADER_ID,
JE_LINE_NUM,
LEDGER_ID,
LEDGER_ID_KEY,
PERIOD_SET_NAME,
CHART_OF_ACCOUNTS_ID,
PERIOD_NAME,
CODE_COMBINATION_ID,
CODE_COMBINATION_ID_KEY,
ACCOUNT_TYPE
,company
,department
,Account
,budget_id
,intercompany
,"LOCATION"
,"JOURNAL_LINE_STATUS",
"JOURNAL_HEADER_STATUS",
"JOURNAL_BATCH_STATUS",
EFFECTIVE_DATE,
LINE_DESCRIPTION,
HEADER_DESCRIPTION,
JE_CATEGORY,
"JOURNAL_SOURCE",
"JOURNAL_NAME",
ACTUAL_FLAG,
"BATCH_NAME",
batch_description
,posted_by
,ORG_ID,
ORG_ID_KEY,
JE_BATCH_ID,
"POSTED_DATE",
LAST_UPDATED_BY,
CREATED_BY,
"LINE_LAST_UPDATE",
"LINE_CREATION_DATE",
"DATE_CREATED",
JE_FROM_SLA_FLAG,
TAX_STATUS_CODE,
CURRENCY_CONVERSION_RATE,
CURRENCY_CODE,
external_reference
, running_total_accounted_cr
, running_total_accounted_dr,
ENTERED_DR,
ENTERED_CR,
ACCOUNTED_DR,
ACCOUNTED_CR,
REFERENCE_1,
REFERENCE_2,
REFERENCE_3,
REFERENCE_4,
REFERENCE_5,
REFERENCE_6,
REFERENCE_7,
REFERENCE_8,
REFERENCE_9,
REFERENCE_10,
GL_SL_LINK_ID,
GL_SL_LINK_TABLE,
project_number
,task_number
,expnd_type,
IS_DELETED_FLG,
orig_transaction_reference,
source_app_id,
dw_load_id,
dw_insert_date,
dw_update_date
)
(
select
JL.JE_HEADER_ID,
JL.JE_LINE_NUM,
JL.LEDGER_ID,
(
select
system_id
from
bec_etl_ctrl.etlsourceappid
where
source_system = 'EBS')|| '-' || JL.LEDGER_ID as LEDGER_ID_KEY,
GL.PERIOD_SET_NAME,
GCC.CHART_OF_ACCOUNTS_ID,
JL.PERIOD_NAME "PERIOD_NAME",
GCC.CODE_COMBINATION_ID,
(
select
system_id
from
bec_etl_ctrl.etlsourceappid
where
source_system = 'EBS')|| '-' || GCC.CODE_COMBINATION_ID as CODE_COMBINATION_ID_KEY,
GCC.ACCOUNT_TYPE
,gcc.segment1  company
,gcc.segment2  department
,gcc.segment3  Account
,gcc.segment5  budget_id
,gcc.segment4  intercompany
,gcc.segment6  "LOCATION",
JL.STATUS "JOURNAL_LINE_STATUS",
JH.STATUS "JOURNAL_HEADER_STATUS",
JB.STATUS "JOURNAL_BATCH_STATUS",
JL.EFFECTIVE_DATE,
JL.DESCRIPTION as LINE_DESCRIPTION,
JH.DESCRIPTION as HEADER_DESCRIPTION,
JH.JE_CATEGORY,
JH.JE_SOURCE "JOURNAL_SOURCE",
JH.NAME "JOURNAL_NAME",
JH.ACTUAL_FLAG,
JB.NAME "BATCH_NAME"
,jb.description batch_description
,jb.posted_by,
JB.ORG_ID as ORG_ID,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||JB.ORG_ID as ORG_ID_KEY,
JH.JE_BATCH_ID,
JH.POSTED_DATE "POSTED_DATE",
JL.LAST_UPDATED_BY,
JL.CREATED_BY,
JL.LAST_UPDATE_DATE "LINE_LAST_UPDATE",
JL.CREATION_DATE "LINE_CREATION_DATE",
JH.DATE_CREATED "DATE_CREATED",
JH.JE_FROM_SLA_FLAG,
JH.TAX_STATUS_CODE,
JH.CURRENCY_CONVERSION_RATE,
JH.CURRENCY_CODE
,jh.external_reference
,jh.running_total_accounted_cr
,jh.running_total_accounted_dr,
JL.ENTERED_DR,
JL.ENTERED_CR,
JL.ACCOUNTED_DR,
JL.ACCOUNTED_CR,
JL.REFERENCE_1,
JL.REFERENCE_2,
JL.REFERENCE_3,
JL.REFERENCE_4,
JL.REFERENCE_5,
JL.REFERENCE_6,
JL.REFERENCE_7,
JL.REFERENCE_8,
JL.REFERENCE_9,
JL.REFERENCE_10,
JL.GL_SL_LINK_ID,
JL.GL_SL_LINK_TABLE 
,JL.ATTRIBUTE1 project_number
,JL.ATTRIBUTE2 task_number
,JL.ATTRIBUTE3 expnd_type
,'N' AS IS_DELETED_FLG
,JL.PERIOD_NAME||JH.NAME||JL.JE_LINE_NUM as orig_transaction_reference,
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
) || '-' || nvl(GL.LEDGER_ID, 0) 
|| '-' || nvl(JL.JE_HEADER_ID, 0) || '-' || nvl(JL.JE_LINE_NUM, 0) 
|| '-' || nvl(JB.JE_BATCH_ID, 0) || '-' ||
nvl(GCC.CODE_COMBINATION_ID, 0)
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
and (
JL.kca_seq_date > (
select
(executebegints-prune_days)
from
bec_etl_ctrl.batch_dw_info
where
dw_table_name = 'fact_gl_journals'
and batch_name = 'gl')
or 
GCC.kca_seq_date > (
select
(executebegints-prune_days)
from
bec_etl_ctrl.batch_dw_info
where
dw_table_name = 'fact_gl_journals'
and batch_name = 'gl'))
);

commit;
end;

update
bec_etl_ctrl.batch_dw_info
set
last_refresh_date = getdate()
where
dw_table_name = 'fact_gl_journals'
and batch_name = 'gl';

commit;