/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for TEMPORARY STAGING TABLE.
# File Version: KPI v1.0
*/

begin;
-- Delete Records

Truncate table bec_dwh.FACT_GL_JOURNAL_STG;
-- Insert records

insert into bec_dwh.FACT_GL_JOURNAL_STG
(created_by_id,
	JE_SOURCE_NAME,
	GL_BATCH_NAME,
	POSTED_DATE,
	je_header_id,
	GL_JE_NAME,
	REVERSAL_PERIOD,
	REVERSAL_STATUS,
	EXTERNAL_REFERENCE,
	GL_LINE_NUMBER,
	effective_date,
	LEDGER_ID,
	LEDGER_SHORT_NAME,
	LEDGER_DESCRIPTION,
	LEDGER_NAME,
	LEDGER_CURRENCY,
	PERIOD_YEAR,
	PERIOD_NUMBER,
	PERIOD_NAME,
	PERIOD_START_DATE,
	PERIOD_END_DATE,
	BALANCE_TYPE_CODE,
	BALANCE_TYPE,
	BUDGET_NAME,
	ENCUMBRANCE_TYPE,
	BEGIN_BALANCE_DR,
	BEGIN_BALANCE_CR,
	PERIOD_NET_DR,
	PERIOD_NET_CR,
	CODE_COMBINATION_ID,
	ACCOUNTING_CODE_COMBINATION,
	CODE_COMBINATION_DESCRIPTION,
	CONTROL_ACCOUNT_FLAG,
	CONTROL_ACCOUNT,
	BALANCING_SEGMENT,
	NATURAL_ACCOUNT_SEGMENT,
	COST_CENTER_SEGMENT,
	MANAGEMENT_SEGMENT,
	INTERCOMPANY_SEGMENT,
	BALANCING_SEGMENT_DESC,
	NATURAL_ACCOUNT_DESC,
	COST_CENTER_DESC,
	MANAGEMENT_SEGMENT_DESC,
	INTERCOMPANY_SEGMENT_DESC,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	segment8,
	segment9,
	segment10,
	BEGIN_RUNNING_TOTAL_CR,
	BEGIN_RUNNING_TOTAL_DR,
	END_RUNNING_TOTAL_CR,
	END_RUNNING_TOTAL_DR,
	LEGAL_ENTITY_ID,
	LEGAL_ENTITY_NAME,
	LE_ADDRESS_LINE_1,
	LE_ADDRESS_LINE_2,
	LE_ADDRESS_LINE_3,
	LE_CITY,
	LE_REGION_1,
	LE_REGION_2,
	LE_REGION_3,
	LE_POSTAL_CODE,
	LE_COUNTRY,
	LE_REGISTRATION_NUMBER,
	LE_REGISTRATION_EFFECTIVE_FROM,
	LE_BR_DAILY_INSCRIPTION_NUMBER,
	LE_BR_DAILY_INSCRIPTION_DATE,
	LE_BR_DAILY_ENTITY,
	LE_BR_DAILY_LOCATION,
	LE_BR_DIRECTOR_NUMBER,
	LE_BR_ACCOUNTANT_NUMBER,
	LE_BR_ACCOUNTANT_NAME,
	user_je_source_name,
	gl_sl_link_id,
	gl_sl_link_table,
	budget_version_id,
	encumbrance_type_id,
	approver_name,
	TRANSLATED_FLAG
	,is_deleted_flg
	,source_app_id
	,dw_load_id
	,dw_insert_date
	,dw_update_date)
(

With CTE_gl_je_sources_tl as (
	
	Select gjst.user_je_source_name,
	gjst.user_je_source_name JE_SOURCE_NAME
	From (select * from bec_ods.gl_je_sources_tl where is_deleted_flg <> 'Y') gjst
	Where gjst.LANGUAGE = 'US'
	),
	
	CTE_gl_import_references as(
	
	Select gir.je_line_num,
	gir.je_header_id,
	gir.je_batch_id,
	gir.gl_sl_link_id, 
	gir.gl_sl_link_table
	From (select * from bec_ods.gl_import_references where is_deleted_flg <> 'Y') gir
	Where (gir.kca_seq_date >= (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_gl_journal_stg' 
	and batch_name = 'gl'))
	),
	
	CTE_gl_je_lines as (
	
	Select gjl.ledger_id,
	gjl.code_combination_id,
	gjl.effective_date,
	gjl.period_name,
	gjl.je_header_id,
	gjl.je_line_num GL_LINE_NUMBER,
	gjl.created_by as created_by_id
	From (select * from bec_ods.gl_je_lines where is_deleted_flg <> 'Y') gjl
	Where (gjl.last_update_date >= to_date('2020-01-01 12:00:00','YYYY-MM-DD HH:MI:SS')
	and gjl.kca_seq_date >= (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_gl_journal_stg' 
	and batch_name = 'gl'))
	),
	
	CTE_gl_je_headers as (
	
	SELECT  gjh.je_header_id,
	gjh.period_name,
	gjh.je_source,
	gjh.NAME GL_JE_NAME,
	gjh.ACCRUAL_REV_PERIOD_NAME REVERSAL_PERIOD,
	gjh.ACCRUAL_REV_STATUS REVERSAL_STATUS,
	gjh.external_reference EXTERNAL_REFERENCE
	From (select * from bec_ods.gl_je_headers where is_deleted_flg <> 'Y') gjh
	Where gjh.status = 'P'
	AND NVL(gjh.je_from_sla_flag, 'N') IN ('Y', 'U')
	AND (gjh.last_update_date >= to_date('2020-01-01 12:00:00','YYYY-MM-DD HH:MI:SS')
	and gjh.kca_seq_date >= (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_gl_journal_stg' 
	and batch_name = 'gl'))
	),
	
	CTE_gl_je_batches as (
	
	Select gjb.je_batch_id,
	gjb.APPROVER_EMPLOYEE_ID,
	gjb.creation_date,
	gjb.NAME GL_BATCH_NAME ,
	gjb.posted_date
	From (select * from bec_ods.gl_je_batches where is_deleted_flg <> 'Y') gjb
	Where gjb.status = 'P'
	AND (gjb.kca_seq_date >= (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_gl_journal_stg' 
	and batch_name = 'gl'))
	),
	
	CTE_FACT_XLA_REPORT_BALANCES_STG as (
	
	Select glbgt.ledger_id,
	glbgt.code_combination_id,
	glbgt.period_start_date as glbgt_period_start_date,
	glbgt.period_end_date as glbgt_period_end_date,
	glbgt.period_name,
	glbgt.balance_type_code,
	glbgt.budget_version_id,
	glbgt.encumbrance_type_id,
	glbgt.ledger_short_name LEDGER_SHORT_NAME,
	glbgt.ledger_description LEDGER_DESCRIPTION,
	glbgt.ledger_name LEDGER_NAME,
	glbgt.ledger_currency LEDGER_CURRENCY,
	glbgt.period_year PERIOD_YEAR,
	glbgt.period_number PERIOD_NUMBER,
	glbgt.period_start_date,
	glbgt.period_end_date,
	glbgt.balance_type BALANCE_TYPE,
	glbgt.budget_name BUDGET_NAME,
	glbgt.encumbrance_type ENCUMBRANCE_TYPE,
	glbgt.begin_balance_dr BEGIN_BALANCE_DR,
	glbgt.begin_balance_cr BEGIN_BALANCE_CR,
	glbgt.period_net_dr PERIOD_NET_DR,
	glbgt.period_net_cr PERIOD_NET_CR,
	'NA'::VARCHAR(2) as ACCOUNTING_CODE_COMBINATION,
	'NA'::VARCHAR(2) as CODE_COMBINATION_DESCRIPTION,
	glbgt.control_account_flag CONTROL_ACCOUNT_FLAG,
	glbgt.control_account CONTROL_ACCOUNT,
	'NA'::VARCHAR(2) as BALANCING_SEGMENT,
	'NA'::VARCHAR(2) as NATURAL_ACCOUNT_SEGMENT,
	'NA'::VARCHAR(2) as COST_CENTER_SEGMENT,
	'NA'::VARCHAR(2) as MANAGEMENT_SEGMENT,
	'NA'::VARCHAR(2) as INTERCOMPANY_SEGMENT,
	'NA'::VARCHAR(2) as BALANCING_SEGMENT_DESC,
	'NA'::VARCHAR(2) as NATURAL_ACCOUNT_DESC,
	'NA'::VARCHAR(2) as COST_CENTER_DESC,
	'NA'::VARCHAR(2) as MANAGEMENT_SEGMENT_DESC,
	'NA'::VARCHAR(2) as INTERCOMPANY_SEGMENT_DESC,
	glbgt.segment1 SEGMENT1,
	glbgt.segment2 SEGMENT2,
	glbgt.segment3 SEGMENT3,
	glbgt.segment4 SEGMENT4,
	glbgt.segment5 SEGMENT5,
	glbgt.segment6 SEGMENT6,
	glbgt.segment7 SEGMENT7,
	glbgt.segment8 SEGMENT8,
	glbgt.segment9 SEGMENT9,
	glbgt.segment10 SEGMENT10,
	'NA'::VARCHAR(2) as BEGIN_RUNNING_TOTAL_CR,
	'NA'::VARCHAR(2) as BEGIN_RUNNING_TOTAL_DR,
	'NA'::VARCHAR(2) as END_RUNNING_TOTAL_CR,
	'NA'::VARCHAR(2) as END_RUNNING_TOTAL_DR,
	'NA'::VARCHAR(2) as LEGAL_ENTITY_ID,
	'NA'::VARCHAR(2) as LEGAL_ENTITY_NAME,
	'NA'::VARCHAR(2) as LE_ADDRESS_LINE_1,
	'NA'::VARCHAR(2) as LE_ADDRESS_LINE_2,
	'NA'::VARCHAR(2) as LE_ADDRESS_LINE_3,
	'NA'::VARCHAR(2) as LE_CITY,
	'NA'::VARCHAR(2) as LE_REGION_1,
	'NA'::VARCHAR(2) as LE_REGION_2,
	'NA'::VARCHAR(2) as LE_REGION_3,
	'NA'::VARCHAR(2) as LE_POSTAL_CODE,
	'NA'::VARCHAR(2) as LE_COUNTRY,
	'NA'::VARCHAR(2) as LE_REGISTRATION_NUMBER,
	'NA'::VARCHAR(2) as LE_REGISTRATION_EFFECTIVE_FROM,
	'NA'::VARCHAR(2) as LE_BR_DAILY_INSCRIPTION_NUMBER,
	NULL::TIMESTAMP as LE_BR_DAILY_INSCRIPTION_DATE,
	'NA'::VARCHAR(2) as LE_BR_DAILY_ENTITY,
	'NA'::VARCHAR(2) as LE_BR_DAILY_LOCATION,
	'NA'::VARCHAR(2) as LE_BR_DIRECTOR_NUMBER,
	'NA'::VARCHAR(2) as LE_BR_ACCOUNTANT_NUMBER,
	'NA'::VARCHAR(2) as LE_BR_ACCOUNTANT_NAME,
	glbgt.TRANSLATED_FLAG
	From bec_dwh.FACT_XLA_REPORT_BALANCES_STG glbgt
	),
	
	CTE_PER_ALL_PEOPLE_F as (
	
	SELECT papf.person_id,
	papf.effective_start_date,
	papf.effective_end_date,
	papf.full_name approver_name
	From (select * from bec_ods.PER_ALL_PEOPLE_F where is_deleted_flg <> 'Y') papf
	)
	
	SELECT
	gjl.created_by_id,
	gjst.JE_SOURCE_NAME,
	gjb.GL_BATCH_NAME,
	gjb.POSTED_DATE,
	gjh.je_header_id,
	gjh.GL_JE_NAME,
	gjh.REVERSAL_PERIOD,
	gjh.REVERSAL_STATUS,
	gjh.EXTERNAL_REFERENCE,
	gjl.GL_LINE_NUMBER,
	gjl.effective_date,
	glbgt.LEDGER_ID,
	glbgt.LEDGER_SHORT_NAME,
	glbgt.LEDGER_DESCRIPTION,
	glbgt.LEDGER_NAME,
	glbgt.LEDGER_CURRENCY,
	glbgt.PERIOD_YEAR,
	glbgt.PERIOD_NUMBER,
	glbgt.PERIOD_NAME,
	glbgt.PERIOD_START_DATE,
	glbgt.PERIOD_END_DATE,
	glbgt.BALANCE_TYPE_CODE,
	glbgt.BALANCE_TYPE,
	glbgt.BUDGET_NAME,
	glbgt.ENCUMBRANCE_TYPE,
	glbgt.BEGIN_BALANCE_DR,
	glbgt.BEGIN_BALANCE_CR,
	glbgt.PERIOD_NET_DR,
	glbgt.PERIOD_NET_CR,
	glbgt.CODE_COMBINATION_ID,
	glbgt.ACCOUNTING_CODE_COMBINATION,
	glbgt.CODE_COMBINATION_DESCRIPTION,
	glbgt.CONTROL_ACCOUNT_FLAG,
	glbgt.CONTROL_ACCOUNT,
	glbgt.BALANCING_SEGMENT,
	glbgt.NATURAL_ACCOUNT_SEGMENT,
	glbgt.COST_CENTER_SEGMENT,
	glbgt.MANAGEMENT_SEGMENT,
	glbgt.INTERCOMPANY_SEGMENT,
	glbgt.BALANCING_SEGMENT_DESC,
	glbgt.NATURAL_ACCOUNT_DESC,
	glbgt.COST_CENTER_DESC,
	glbgt.MANAGEMENT_SEGMENT_DESC,
	glbgt.INTERCOMPANY_SEGMENT_DESC,
	glbgt.segment1 SEGMENT1,
	glbgt.segment2 SEGMENT2,
	glbgt.segment3 SEGMENT3,
	glbgt.segment4 SEGMENT4,
	glbgt.segment5 SEGMENT5,
	glbgt.segment6 SEGMENT6,
	glbgt.segment7 SEGMENT7,
	glbgt.segment8 SEGMENT8,
	glbgt.segment9 SEGMENT9,
	glbgt.segment10 SEGMENT10,
	glbgt.BEGIN_RUNNING_TOTAL_CR,
	glbgt.BEGIN_RUNNING_TOTAL_DR,
	glbgt.END_RUNNING_TOTAL_CR,
	glbgt.END_RUNNING_TOTAL_DR,
	glbgt.LEGAL_ENTITY_ID,
	glbgt.LEGAL_ENTITY_NAME,
	glbgt.LE_ADDRESS_LINE_1,
	glbgt.LE_ADDRESS_LINE_2,
	glbgt.LE_ADDRESS_LINE_3,
	glbgt.LE_CITY,
	glbgt.LE_REGION_1,
	glbgt.LE_REGION_2,
	glbgt.LE_REGION_3,
	glbgt.LE_POSTAL_CODE,
	glbgt.LE_COUNTRY,
	glbgt.LE_REGISTRATION_NUMBER,
	glbgt.LE_REGISTRATION_EFFECTIVE_FROM,
	glbgt.LE_BR_DAILY_INSCRIPTION_NUMBER,
	glbgt.LE_BR_DAILY_INSCRIPTION_DATE,
	glbgt.LE_BR_DAILY_ENTITY,
	glbgt.LE_BR_DAILY_LOCATION,
	glbgt.LE_BR_DIRECTOR_NUMBER,
	glbgt.LE_BR_ACCOUNTANT_NUMBER,
	glbgt.LE_BR_ACCOUNTANT_NAME,
	gjst.user_je_source_name,
	gir.gl_sl_link_id,
	gir.gl_sl_link_table,
	glbgt.budget_version_id,
	glbgt.encumbrance_type_id,
	papf.approver_name,
	glbgt.TRANSLATED_FLAG
	-- audit columns
	,'N' as is_deleted_flg
	,(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id
	,(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')
	|| '-' || nvl(gir.gl_sl_link_id, 0)
	|| '-' || nvl(glbgt.LEDGER_CURRENCY, 'NA')	
	|| '-' || nvl(glbgt.TRANSLATED_FLAG, 'NA')	as dw_load_id
	,getdate() as dw_insert_date
	,getdate() as dw_update_date
	FROM
	CTE_gl_je_sources_tl gjst,
	CTE_gl_import_references gir,
	CTE_gl_je_lines gjl,
	CTE_gl_je_headers gjh,
	CTE_gl_je_batches gjb,
	CTE_FACT_XLA_REPORT_BALANCES_STG glbgt,
	CTE_PER_ALL_PEOPLE_F papf
	Where 1=1
	AND gjl.ledger_id = glbgt.ledger_id
	AND gjl.code_combination_id = glbgt.code_combination_id
	AND gjl.effective_date BETWEEN glbgt.glbgt_period_start_date AND glbgt.glbgt_period_end_date
	AND gjl.period_name = glbgt.period_name
	AND gjl.je_header_id = gjh.je_header_id
	AND gjl.period_name = gjh.period_name
	AND gjl.je_header_id = gir.je_header_id
	AND gjl.GL_LINE_NUMBER = gir.je_line_num
	AND gjh.je_header_id = gir.je_header_id
	AND gjb.je_batch_id = gir.je_batch_id
	AND gjb.APPROVER_EMPLOYEE_ID = papf.person_id(+)
	AND gjb.creation_date BETWEEN papf.effective_start_date (+) AND NVL(papf.effective_end_date (+), GETDATE() + 1)
	AND gjst.je_source_name = gjh.je_source
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_gl_journal_stg'
	and batch_name = 'gl';

commit;