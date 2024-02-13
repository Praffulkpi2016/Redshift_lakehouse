/*
# COPYRIGHT(C) 2022 KPI PARTNERS, INC. ALL RIGHTS RESERVED.
#
# UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING, SOFTWARE
# DISTRIBUTED UNDER THE LICENSE IS DISTRIBUTED ON AN "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED.
#
# AUTHOR: KPI PARTNERS, INC.
# VERSION: 2022.06
# DESCRIPTION: THIS SCRIPT REPRESENTS FULL LOAD APPROACH FOR FACTS.
# FILE VERSION: KPI V1.0
*/

BEGIN;
drop table if exists bec_dwh.FACT_JOURNAL_LINES;
create table  bec_dwh.fact_journal_lines diststyle all sortkey(ledger_id,je_batch_id,je_header_id,currency_code,je_line_num,gl_period,
code_combination_id)
as
(
select 
	jl.je_header_id as je_header_id,
	jl.je_line_num as je_line_num,
	jl.ledger_id as ledger_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||jl.ledger_id ledger_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||jl.je_header_id je_header_id_key,	
	gl.period_set_name as period_set_name,
	gcc.chart_of_accounts_id||'-'||gcc.segment1 as coa_segval1,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment2, '000') as coa_segval2,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment3, '000') as coa_segval3,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment4, '000') as coa_segval4,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment5, '000') as coa_segval5,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment6, '000') as coa_segval6,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment7, '000') as coa_segval7,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment8, '000') as coa_segval8,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment9, '000') as coa_segval9,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment10, '000') as coa_segval10,
	gcc.chart_of_accounts_id as chart_of_accounts_id,
	gcc.segment1 as segval1,
	gcc.segment2 as segval2,
	gcc.segment3 as segval3,
	gcc.segment4 as segval4,
	gcc.segment5 as segval5,
	gcc.segment6 as segval6,
	gcc.segment7 as segval7,
	gcc.segment8 as segval8,
	gcc.segment9 as segval9,
	gcc.segment10 as segval10,
	jl.ledger_id||'-'||upper(jl.period_name) as	period_name,
	jl.period_name as gl_period,
	gcc.code_combination_id as code_combination_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| gcc.code_combination_id code_combination_id_key,	
	gcc.account_type as	account_type,
	gbv.budget_name as actual_or_budget,
	jl.status as journal_line_status,
	jh.status as journal_header_status,
	jb.status as journal_batch_status,
	jl.effective_date as effective_date,
	jl.description as description,
	jl.entered_dr as entered_dr,
	jl.entered_cr as entered_cr,
	jl.accounted_dr as accounted_dr,
	jl.accounted_cr as accounted_cr,
	jh.je_category as je_category,
	jh.je_source as journal_source,
	jh.name as journal_name,
	jh.currency_code as currency_code,
	cast(jh.date_created as date) as date_created,
	jh.actual_flag as actual_flag,
	jb.name as batch_name,
	jh.je_batch_id as je_batch_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| jh.je_batch_id je_batch_id_key,
	cast(jh.posted_date as date) as posted_date,
	jl.last_updated_by as last_updated_by,
	jl.created_by as created_by,
	cast(jl.last_update_date as date) as line_last_update,
	cast(jl.creation_date as date) as line_creation_date,
	jh.description header_desc,
	jh.accrual_rev_flag Reversal_Flag,
	jh.accrual_rev_status Reversal_Status,
	jh.accrual_rev_je_header_id Reversal_header_id,
	jh.accrual_rev_period_name Reversal_Period,
	jh.accrual_rev_effective_date Reversal_effective_date,
	jl.attribute1 ,
	jl.attribute2 , 
	jl.attribute3 ,
	jb.approval_status_code,
	jb.approver_employee_id,	
	jh.CURRENCY_CONVERSION_RATE,
	jh.CURRENCY_CONVERSION_TYPE,
	jh.CURRENCY_CONVERSION_DATE,
	'N' as is_deleted_flg,
	--newly added column
	jb.posted_by,
	-- audit columns
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(jl.ledger_id,0)
		||'-'||nvl(jh.je_batch_id,0)||'-'||nvl(jh.je_header_id,0)||'-'||nvl(jh.currency_code,'NA')||'-'||
		nvl(jl.je_line_num,0)||'-'||upper(nvl(jl.period_name,'NA'))||'-'||nvl(gcc.code_combination_id,0)
		||'-'||nvl(jh.actual_flag,'X') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date 
from (select * from bec_ods.gl_je_lines where is_deleted_flg<>'Y') jl
inner join (select * from bec_ods.gl_je_headers where is_deleted_flg<>'Y') jh
on
	jh.je_header_id = jl.je_header_id
inner join (select * from bec_ods.gl_je_batches where is_deleted_flg<>'Y') jb
on
	jh.je_batch_id = jb.je_batch_id
inner join (select * from bec_ods.gl_code_combinations where is_deleted_flg<>'Y') gcc
on
	gcc.code_combination_id = jl.code_combination_id 
inner join (select * from bec_ods.gl_budget_versions where is_deleted_flg<>'Y') gbv
on
	jh.budget_version_id = gbv.budget_version_id 
inner join (select * from bec_ods.gl_ledgers where is_deleted_flg<>'Y') gl
on	
	jl.ledger_id = gl.ledger_id
where 1=1
	and jh.actual_flag = 'B'  
	and gcc.summary_flag = 'N'

union all 

select 
	jl.je_header_id as je_header_id,
	jl.je_line_num as je_line_num,
	jl.ledger_id as ledger_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||jl.ledger_id ledger_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||jl.je_header_id je_header_id_key,
	gl.period_set_name as period_set_name,
	gcc.chart_of_accounts_id||'-'||gcc.segment1 as coa_segval1,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment2, '000') as coa_segval2,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment3, '000') as coa_segval3,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment4, '000') as coa_segval4,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment5, '000') as coa_segval5,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment6, '000') as coa_segval6,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment7, '000') as coa_segval7,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment8, '000') as coa_segval8,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment9, '000') as coa_segval9,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment10, '000') as coa_segval10,
	gcc.chart_of_accounts_id as chart_of_accounts_id,
	gcc.segment1 as segval1,
	gcc.segment2 as segval2,
	gcc.segment3 as segval3,
	gcc.segment4 as segval4,
	gcc.segment5 as segval5,
	gcc.segment6 as segval6,
	gcc.segment7 as	segval7,
	gcc.segment8 as	segval8,
	gcc.segment9 as	segval9,
	gcc.segment10 as segval10,
	jl.ledger_id||'-'||upper(jl.period_name) as	period_name,
	jl.period_name as gl_period,
	gcc.code_combination_id as code_combination_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| gcc.code_combination_id code_combination_id_key,
	gcc.account_type as	account_type,
	'ACTUAL' as	actual_or_budget,
	jl.status as journal_line_status,
	jh.status as journal_header_status,
	jb.status as journal_batch_status,
	jl.effective_date as effective_date,
	jl.description as description,
	jl.entered_dr as entered_dr,
	jl.entered_cr as entered_cr,
	jl.accounted_dr as accounted_dr,
	jl.accounted_cr as accounted_cr,
	jh.je_category as je_category,
	jh.je_source as	journal_source,
	jh.name as journal_name,
	jh.currency_code as	currency_code,
	cast(jh.date_created as date) as date_created,
	jh.actual_flag as actual_flag,
	jb.name as batch_name,
	jh.je_batch_id as je_batch_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| jh.je_batch_id je_batch_id_key,
	cast(jh.posted_date as date) as	posted_date,
	jl.last_updated_by as last_updated_by,
	jl.created_by as created_by,
	cast(jl.last_update_date as date) as line_last_update,
	cast(jl.creation_date as date) as line_creation_date,
	jh.description header_desc,
	jh.accrual_rev_flag Reversal_Flag,
	jh.accrual_rev_status Reversal_Status,
	jh.accrual_rev_je_header_id Reversal_header_id,
	jh.accrual_rev_period_name Reversal_Period,
	jh.accrual_rev_effective_date Reversal_effective_date,
	jl.attribute1 ,
	jl.attribute2 , 
	jl.attribute3 ,
	jb.approval_status_code,
	jb.approver_employee_id,
	jh.CURRENCY_CONVERSION_RATE,
	jh.CURRENCY_CONVERSION_TYPE,
	jh.CURRENCY_CONVERSION_DATE,
	'N' as is_deleted_flg,
    --newly added column
	jb.posted_by,
	-- audit columns
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(jl.ledger_id,0)
		||'-'||nvl(jh.je_batch_id,0)||'-'||nvl(jh.je_header_id,0)||'-'||nvl(jh.currency_code,'NA')||'-'||
		nvl(jl.je_line_num,0)||'-'||upper(nvl(jl.period_name,'NA'))||'-'||nvl(gcc.code_combination_id,0)
		||'-'||nvl(jh.actual_flag,'X') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from (select * from bec_ods.gl_je_lines where is_deleted_flg<>'Y') jl
inner join (select * from bec_ods.gl_je_headers where is_deleted_flg<>'Y') jh
on 
	jh.je_header_id = jl.je_header_id 
inner join (select * from bec_ods.gl_je_batches where is_deleted_flg<>'Y') jb 
on 
	jh.je_batch_id = jb.je_batch_id 
inner join (select * from bec_ods.gl_code_combinations where is_deleted_flg<>'Y') gcc 
on 
	gcc.code_combination_id = jl.code_combination_id 
inner join (select * from bec_ods.gl_ledgers where is_deleted_flg<>'Y') gl
on 
	jl.ledger_id = gl.ledger_id
where 1=1
	and jh.actual_flag = 'A' 
	and gcc.summary_flag = 'N'
);
END;

UPDATE
	BEC_ETL_CTRL.BATCH_DW_INFO
SET
	LOAD_TYPE = 'I',
	LAST_REFRESH_DATE = GETDATE()
WHERE
	DW_TABLE_NAME = 'fact_journal_lines'
	AND BATCH_NAME = 'gl';

COMMIT;