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
	bec_dwh.FACT_JOURNAL_LINES
	where
	(nvl(ledger_id,0),
		nvl(je_batch_id,0),
		nvl(je_header_id,0),
		nvl(currency_code,'NA'),
		nvl(je_line_num,0),
		nvl(gl_period,'NA'),
	nvl(code_combination_id,0),
	nvl(actual_flag,'X')) in (
		select
		nvl(ods.ledger_id,0) as ledger_id,
		nvl(ods.je_batch_id,0) as je_batch_id,
		nvl(ods.je_header_id,0) as je_header_id,
		nvl(ods.currency_code,'NA') as currency_code,
		nvl(ods.je_line_num,0) as je_line_num,
		nvl(ods.gl_period,'NA') as gl_period,
		nvl(ods.code_combination_id,0) as code_combination_id,
		nvl(ods.actual_flag,'NA') as actual_flag
		from
		bec_dwh.FACT_JOURNAL_LINES dw,
		(
			select
			jl.je_header_id as je_header_id,
			jl.je_line_num as je_line_num,
			jl.ledger_id as ledger_id,
			gl.period_set_name as period_set_name,
			gcc.chart_of_accounts_id as chart_of_accounts_id,
			jl.period_name as gl_period,
			gcc.code_combination_id as code_combination_id,
			jh.je_category as je_category,
			jh.je_source as journal_source,
			jh.je_batch_id as je_batch_id,
			jh.currency_code as currency_code,
			jh.actual_flag as actual_flag,
			jb.posted_by,
			jl.last_update_date
			from
			bec_ods.gl_je_lines   jl
			inner join   bec_ods.gl_je_headers   jh
			on
			jh.je_header_id = jl.je_header_id
			inner join   bec_ods.gl_je_batches   jb
			on
			jh.je_batch_id = jb.je_batch_id
			inner join   bec_ods.gl_code_combinations   gcc
			on
			gcc.code_combination_id = jl.code_combination_id
			inner join   bec_ods.gl_budget_versions   gbv
			on
			jh.budget_version_id = gbv.budget_version_id
			inner join   bec_ods.gl_ledgers   gl
			on
			jl.ledger_id = gl.ledger_id
			where
			1 = 1
			and jh.actual_flag = 'B'
			and gcc.summary_flag = 'N'
			and (jl.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_journal_lines'
			and batch_name = 'gl') or jl.is_deleted_flg = 'Y'
			or jh.is_deleted_flg = 'Y'
			or jb.is_deleted_flg = 'Y'
			or gcc.is_deleted_flg = 'Y'
			or gl.is_deleted_flg = 'Y' 
			or gbv.is_deleted_flg = 'Y')
			union all
			select
			jl.je_header_id as je_header_id,
			jl.je_line_num as je_line_num,
			jl.ledger_id as ledger_id,
			gl.period_set_name as period_set_name,
			gcc.chart_of_accounts_id as chart_of_accounts_id,
			jl.period_name as gl_period,
			gcc.code_combination_id as code_combination_id,
			jh.je_category as je_category,
			jh.je_source as journal_source,
			jh.je_batch_id as je_batch_id,
			jh.currency_code as currency_code,
			jh.actual_flag as actual_flag,
			jb.posted_by,
			jl.last_update_date
			from
			bec_ods.gl_je_lines   jl
			inner join   bec_ods.gl_je_headers   jh
			on
			jh.je_header_id = jl.je_header_id
			inner join   bec_ods.gl_je_batches  jb 
			on
			jh.je_batch_id = jb.je_batch_id
			inner join   bec_ods.gl_code_combinations   gcc 
			on
			gcc.code_combination_id = jl.code_combination_id
			inner join  bec_ods.gl_ledgers   gl
			on
			jl.ledger_id = gl.ledger_id
			where
			1 = 1
			and jh.actual_flag = 'A'
			and gcc.summary_flag = 'N'
			and (jl.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_journal_lines'
			and batch_name = 'gl') or jl.is_deleted_flg = 'Y' 
			or jh.is_deleted_flg = 'Y'
			or jb.is_deleted_flg = 'Y'
			or gcc.is_deleted_flg = 'Y'
			or gl.is_deleted_flg = 'Y'
			 
			 )
		) ods
		where
		dw.dw_load_id = (
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
		where
		source_system = 'EBS')|| '-' || nvl(ods.ledger_id, 0)
		|| '-' || nvl(ods.je_batch_id, 0)|| '-' || nvl(ods.je_header_id, 0)|| '-' || nvl(ods.currency_code, 'NA')|| '-' ||
		nvl(ods.je_line_num, 0)|| '-' || upper(nvl(ods.gl_period, 'NA'))|| '-' || nvl(ods.code_combination_id, 0)
		|| '-' || nvl(ods.actual_flag, 'X')
		);
		
		commit;
		-- Insert records
		
		insert
		into
		bec_dwh.FACT_JOURNAL_LINES
		(   je_header_id,
			je_line_num,
			ledger_id,
			ledger_id_key,
			je_header_id_key,
			period_set_name,
			coa_segval1,
			coa_segval2,
			coa_segval3,
			coa_segval4,
			coa_segval5,
			coa_segval6,
			coa_segval7,
			coa_segval8,
			coa_segval9,
			coa_segval10,
			chart_of_accounts_id,
			segval1,
			segval2,
			segval3,
			segval4,
			segval5,
			segval6,
			segval7,
			segval8,
			segval9,
			segval10,
			period_name,
			gl_period,
			code_combination_id,
			code_combination_id_key,
			account_type,
			actual_or_budget,
			journal_line_status,
			journal_header_status,
			journal_batch_status,
			effective_date,
			description,
			entered_dr,
			entered_cr,
			accounted_dr,
			accounted_cr,
			je_category,
			journal_source,
			journal_name,
			currency_code,
			date_created,
			actual_flag,
			batch_name,
			je_batch_id,
			je_batch_id_key,
			posted_date,
			last_updated_by,
			created_by,
			line_last_update,
			line_creation_date,
			header_desc,
			reversal_flag,
			reversal_status,
			reversal_header_id,
			reversal_period,
			reversal_effective_date,
			attribute1,
			attribute2,
			attribute3,
			approval_status_code,
			approver_employee_id,
			CURRENCY_CONVERSION_RATE,
			CURRENCY_CONVERSION_TYPE,
			CURRENCY_CONVERSION_DATE,
			is_deleted_flg,
			posted_by,
			source_app_id,
			dw_load_id,
			dw_insert_date,
		dw_update_date )
		(
			select
			jl.je_header_id as je_header_id,
			jl.je_line_num as je_line_num,
			jl.ledger_id as ledger_id,
			(
				select
				system_id
				from
				bec_etl_ctrl.etlsourceappid
				where
			source_system = 'EBS')|| '-' || jl.ledger_id ledger_id_key,
			(
				select
				system_id
				from
				bec_etl_ctrl.etlsourceappid
				where
			source_system = 'EBS')|| '-' || jl.je_header_id je_header_id_key,
			gl.period_set_name as period_set_name,
			gcc.chart_of_accounts_id || '-' || gcc.segment1 as coa_segval1,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment2, '000') as coa_segval2,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment3, '000') as coa_segval3,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment4, '000') as coa_segval4,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment5, '000') as coa_segval5,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment6, '000') as coa_segval6,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment7, '000') as coa_segval7,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment8, '000') as coa_segval8,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment9, '000') as coa_segval9,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment10, '000') as coa_segval10,
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
			jl.ledger_id || '-' || upper(jl.period_name) as period_name,
			jl.period_name as gl_period,
			gcc.code_combination_id as code_combination_id,
			(
				select
				system_id
				from
				bec_etl_ctrl.etlsourceappid
				where
			source_system = 'EBS')|| '-' || gcc.code_combination_id code_combination_id_key,
			gcc.account_type as account_type,
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
			(
				select
				system_id
				from
				bec_etl_ctrl.etlsourceappid
				where
			source_system = 'EBS')|| '-' || jh.je_batch_id je_batch_id_key,
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
			-- audit columns
			'N' as is_deleted_flg,
						jb.posted_by,
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
			source_system = 'EBS')|| '-' || nvl(jl.ledger_id, 0)
			|| '-' || nvl(jh.je_batch_id, 0)|| '-' || nvl(jh.je_header_id, 0)|| '-' || nvl(jh.currency_code, 'NA')|| '-' ||
			nvl(jl.je_line_num, 0)|| '-' || upper(nvl(jl.period_name, 'NA'))|| '-' || nvl(gcc.code_combination_id, 0)
			|| '-' || nvl(jh.actual_flag, 'X') as dw_load_id,
			getdate() as dw_insert_date,
			getdate() as dw_update_date
			from
			(select * from bec_ods.gl_je_lines where is_deleted_flg<>'Y') jl
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
			where
			1 = 1
			and jh.actual_flag = 'B'
			and gcc.summary_flag = 'N'
			and jl.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_journal_lines'
			and batch_name = 'gl')
			
			union all
			select
			jl.je_header_id as je_header_id,
			jl.je_line_num as je_line_num,
			jl.ledger_id as ledger_id,
			(
				select
				system_id
				from
				bec_etl_ctrl.etlsourceappid
				where
			source_system = 'EBS')|| '-' || jl.ledger_id ledger_id_key,
			(
				select
				system_id
				from
				bec_etl_ctrl.etlsourceappid
				where
			source_system = 'EBS')|| '-' || jl.je_header_id je_header_id_key,
			gl.period_set_name as period_set_name,
			gcc.chart_of_accounts_id || '-' || gcc.segment1 as coa_segval1,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment2, '000') as coa_segval2,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment3, '000') as coa_segval3,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment4, '000') as coa_segval4,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment5, '000') as coa_segval5,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment6, '000') as coa_segval6,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment7, '000') as coa_segval7,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment8, '000') as coa_segval8,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment9, '000') as coa_segval9,
			gcc.chart_of_accounts_id || '-' || nvl(gcc.segment10, '000') as coa_segval10,
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
			jl.ledger_id || '-' || upper(jl.period_name) as period_name,
			jl.period_name as gl_period,
			gcc.code_combination_id as code_combination_id,
			(
				select
				system_id
				from
				bec_etl_ctrl.etlsourceappid
				where
			source_system = 'EBS')|| '-' || gcc.code_combination_id code_combination_id_key,
			gcc.account_type as account_type,
			'ACTUAL' as actual_or_budget,
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
			(
				select
				system_id
				from
				bec_etl_ctrl.etlsourceappid
				where
			source_system = 'EBS')|| '-' || jh.je_batch_id je_batch_id_key,
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
			-- audit columns
			'N' as is_deleted_flg,
						jb.posted_by,
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
			source_system = 'EBS')|| '-' || nvl(jl.ledger_id, 0)
			|| '-' || nvl(jh.je_batch_id, 0)|| '-' || nvl(jh.je_header_id, 0)|| '-' || nvl(jh.currency_code, 'NA')|| '-' ||
			nvl(jl.je_line_num, 0)|| '-' || upper(nvl(jl.period_name, 'NA'))|| '-' || nvl(gcc.code_combination_id, 0)
			|| '-' || nvl(jh.actual_flag, 'X') as dw_load_id,
			getdate() as dw_insert_date,
			getdate() as dw_update_date
			from
			(select * from bec_ods.gl_je_lines where is_deleted_flg<>'Y') jl
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
			where
			1 = 1
			and jh.actual_flag = 'A'
			and gcc.summary_flag = 'N'
			and jl.kca_seq_date > (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_journal_lines'
			and batch_name = 'gl')
		
	);
	
	commit;
end;
