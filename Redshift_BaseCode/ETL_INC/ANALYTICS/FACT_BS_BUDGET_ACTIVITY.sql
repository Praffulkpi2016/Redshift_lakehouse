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
	bec_dwh.FACT_BS_BUDGET_ACTIVITY
where
	(
	nvl(ledger_id,0),
	nvl(currency_code,'NA'),
	nvl(upper(gl_period),'NA'),
	nvl(actual_flag,'X'),
	nvl(code_combination_id,0),
	nvl(translated_flag, 'X'),
	nvl(budget_version_id, 0)	
	) 
	in 
(
	select
		nvl(ods.ledger_id,0) as ledger_id,
		nvl(ods.currency_code,'NA') as currency_code,
		nvl(upper(ods.period_name),'NA') as gl_period,
		nvl(ods.actual_flag,'X') as actual_flag,
		nvl(ods.code_combination_id,0) as code_combination_id,
		nvl(ods.translated_flag, 'X') as translated_flag,
		nvl(ods.budget_version_id, 0) as budget_version_id		
	from
		bec_dwh.FACT_BS_BUDGET_ACTIVITY dw,
		(	select
	gbs.ledger_id as ledger_id,
	gbs.currency_code as currency_code,
	gbs.code_combination_id as code_combination_id,
	gbs.period_name as period_name,
	gbs.actual_flag,
	gbs.translated_flag,
	gbs.budget_version_id
	from
		(
		select
		gb.ledger_id,
		gb.currency_code,
		upper(gb.period_name) "period_name",
		gb.actual_flag,
		gb.translated_flag,
		gb.budget_version_id,
		gcc.code_combination_id,
		gcc.account_type
	from
	bec_ods.gl_balances  gb
	inner join bec_ods.gl_code_combinations  gcc
on
		gb.code_combination_id = gcc.code_combination_id
	where
		1 = 1
		and gcc.summary_flag = 'N'
		and gb.actual_flag = 'A' 
			and  ( gb.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_bs_budget_activity'
				and batch_name = 'gl')
				OR
			gcc.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_bs_budget_activity'
				and batch_name = 'gl')  
			or gb.is_deleted_flg = 'Y'
			or gcc.is_deleted_flg = 'Y') 
) gbs
	where
		1 = 1
		and gbs.account_type in ('A', 'L', 'O')
union all
	select 
	gbs.ledger_id as ledger_id,
	gbs.currency_code as currency_code,
	gbs.code_combination_id as code_combination_id,
	gbs.period_name as period_name,
	gbs.actual_flag,
	gbs.translated_flag,
	gbs.budget_version_id
	from
		(
		select
		gb.ledger_id,
		gb.currency_code,
		upper(gb.period_name) "period_name",
		gb.actual_flag,
		gb.translated_flag,
		gb.budget_version_id,
		gcc.code_combination_id,
		gcc.account_type
	from
		bec_ods.gl_balances gb
	inner join bec_ods.gl_code_combinations gcc
	on gb.code_combination_id = gcc.code_combination_id
		where
		1 = 1
		and gcc.summary_flag = 'N'
		and gb.actual_flag = 'B'
		and  ( gb.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_bs_budget_activity'
				and batch_name = 'gl')
				OR
			gcc.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_bs_budget_activity'
				and batch_name = 'gl') 
			or gb.is_deleted_flg = 'Y'
			or gcc.is_deleted_flg = 'Y'	)
) gbs
	where
		1 = 1
		and gbs.account_type in ('A', 'L', 'O')	
)ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
		|| '-' || nvl(ods.ledger_id, 0)
		|| '-' || nvl(ods.currency_code, 'NA')
		|| '-' || upper(nvl(ods.period_name, 'NA'))
		|| '-' || nvl(ods.actual_flag, 'X')
		|| '-' || nvl(ods.code_combination_id, 0)
		|| '-' || nvl(ods.translated_flag, 'X')
		|| '-' || nvl(ods.budget_version_id, 0)
		
);

commit;
-- Insert records

insert
	into
	bec_dwh.FACT_BS_BUDGET_ACTIVITY 
	(
	ledger_id,
	chart_of_accounts_id,
	currency_code,
	code_combination_id,
	period_name,
	gl_period,
	period_year,
	actual_flag,
	translated_flag,
	budget_version_id,
	account_type,
	coa_segval3,
	coa_segval2,
	coa_segval1,
	coa_segval4,
	coa_segval5,
	coa_segval6,
	coa_segval7,
	coa_segval8,
	coa_segval9,
	coa_segval10,
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
	actual_amount_in_k,
	budget_amount_in_k,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
	gbs.ledger_id as ledger_id,
	gbs.chart_of_accounts_id as chart_of_accounts_id,
	gbs.currency_code as currency_code,
	gbs.code_combination_id as code_combination_id,
	gbs.ledger_id || '-' || gbs.period_name as period_name,
	gbs.period_name as gl_period,
	gbs.period_year as period_year,
	gbs.actual_flag,
	gbs.translated_flag,
	gbs.budget_version_id,
	gbs.account_type as account_type,
	gbs.coa_segval3 as coa_segval3,
	gbs.coa_segval2 as coa_segval2,
	gbs.coa_segval1 as coa_segval1,
	gbs.coa_segval4 as coa_segval4,
	gbs.coa_segval5 as coa_segval5,
	gbs.coa_segval6 as coa_segval6,
	gbs.coa_segval7 as coa_segval7,
	gbs.coa_segval8 as coa_segval8,
	gbs.coa_segval9 as coa_segval9,
	gbs.coa_segval10 as coa_segval10,
	gbs.segment1 as segment1,
	gbs.segment2 as segment2,
	gbs.segment3 as segment3,
	gbs.segment4 as segment4,
	gbs.segment5 as segment5,
	gbs.segment6 as segment6,
	gbs.segment7 as segment7,
	gbs.segment8 as segment8,
	gbs.segment9 as segment9,
	gbs.segment10 as segment10,
	decode(gbs.actual_flag, 'A', ((-nvl(gbs.period_net_dr, 0) + nvl(gbs.period_net_cr, 0))/ 1000), 0) as actual_amount_in_k,
	0 as budget_amount_in_k,
	-- audit columns
	'N' as is_deleted_flg,
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
		source_system = 'EBS')
		|| '-' || nvl(gbs.ledger_id,0)
		|| '-' || nvl(gbs.currency_code,'NA')
		|| '-' || nvl(upper(gbs.period_name),'NA')
		|| '-' || nvl(gbs.actual_flag,'X')
		|| '-' || nvl(gbs.code_combination_id,0)
		|| '-' || nvl(gbs.translated_flag, 'X')
		|| '-' || nvl(gbs.budget_version_id, 0)
		as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
	from
		(
		select
		gb.ledger_id,
		gb.currency_code,
		upper(gb.period_name) "period_name",
		gb.period_year,
		gb.actual_flag,
		gb.encumbrance_type_id,
		gb.translated_flag,
		gb.budget_version_id,
		gcc.account_type,
		gcc.code_combination_id,
		gcc.chart_of_accounts_id,
		gb.period_num,
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment1, '00') "coa_segval1",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment2, '00') "coa_segval2",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment3, '00') "coa_segval3",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment4, '00') "coa_segval4",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment5, '00') "coa_segval5",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment6, '00') "coa_segval6",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment7, '00') "coa_segval7",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment8, '00') "coa_segval8",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment9, '00') "coa_segval9",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment10, '00') "coa_segval10",
		gcc.segment1,
		gcc.segment2,
		gcc.segment3,
		gcc.segment4,
		gcc.segment5,
		gcc.segment6,
		gcc.segment7,
		gcc.segment8,
		gcc.segment9,
		gcc.segment10,
		gb.period_net_cr,
		gb.period_net_dr,
		gb.quarter_to_date_cr,
		gb.quarter_to_date_dr,
		gb.project_to_date_cr,
		gb.project_to_date_dr,
		gb.begin_balance_cr,
		gb.begin_balance_dr
	from
		(select * from bec_ods.gl_balances where is_deleted_flg <> 'Y')  gb
	inner join (select * from bec_ods.gl_code_combinations where is_deleted_flg <> 'Y')  gcc
on
		gb.code_combination_id = gcc.code_combination_id
	where
		1 = 1
		and gcc.summary_flag = 'N'
		and gb.actual_flag = 'A' 
			and  ( gb.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_bs_budget_activity'
				and batch_name = 'gl')
				or
			gcc.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_bs_budget_activity'
				and batch_name = 'gl')  ) 
) gbs
	where
		1 = 1
		and gbs.account_type in ('A', 'L', 'O')
union all
	select 
	gbs.ledger_id as ledger_id,
	gbs.chart_of_accounts_id as chart_of_accounts_id,
	gbs.currency_code as currency_code,
	gbs.code_combination_id as code_combination_id,
	gbs.ledger_id || '-' || gbs.period_name as period_name,
	gbs.period_name as gl_period,
	gbs.period_year as period_year,
	gbs.actual_flag,
	gbs.translated_flag,
	gbs.budget_version_id,
	gbs.account_type as account_type,
	gbs.coa_segval3 as coa_segval3,
	gbs.coa_segval2 as coa_segval2,
	gbs.coa_segval1 as coa_segval1,
	gbs.coa_segval4 as coa_segval4,
	gbs.coa_segval5 as coa_segval5,
	gbs.coa_segval6 as coa_segval6,
	gbs.coa_segval7 as coa_segval7,
	gbs.coa_segval8 as coa_segval8,
	gbs.coa_segval9 as coa_segval9,
	gbs.coa_segval10 as coa_segval10,
	gbs.segment1 as segment1,
	gbs.segment2 as segment2,
	gbs.segment3 as segment3,
	gbs.segment4 as segment4,
	gbs.segment5 as segment5,
	gbs.segment6 as segment6,
	gbs.segment7 as segment7,
	gbs.segment8 as segment8,
	gbs.segment9 as segment9,
	gbs.segment10 as segment10,
	0 as actual_amount_in_k,
	decode(gbs.actual_flag, 'B', ((-nvl(gbs.period_net_dr, 0) - nvl(gbs.begin_balance_dr, 0) + nvl(gbs.begin_balance_cr, 0)+ nvl(gbs.period_net_cr, 0))/ 1000), 0) as budget_amount_in_k,
	-- audit columns
	'N' as is_deleted_flg,
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
		source_system = 'EBS')
		|| '-' || nvl(gbs.ledger_id,0)
		|| '-' || nvl(gbs.currency_code,'NA')
		|| '-' || nvl(upper(gbs.period_name),'NA')
		|| '-' || nvl(gbs.actual_flag,'X')
		|| '-' || nvl(gbs.code_combination_id,0)
		|| '-' || nvl(gbs.translated_flag, 'X')
		|| '-' || nvl(gbs.budget_version_id, 0)
		as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
	from
		(
		select
		gb.ledger_id,
		gb.currency_code,
		upper(gb.period_name) "period_name",
		gb.period_year,
		gb.encumbrance_type_id,
		gb.actual_flag,
		gb.translated_flag,
		gb.budget_version_id,
		gcc.account_type,
		gcc.code_combination_id,
		gcc.chart_of_accounts_id,
		gb.period_num,
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment1, '00') "coa_segval1",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment2, '00') "coa_segval2",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment3, '00') "coa_segval3",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment4, '00') "coa_segval4",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment5, '00') "coa_segval5",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment6, '00') "coa_segval6",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment7, '00') "coa_segval7",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment8, '00') "coa_segval8",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment9, '00') "coa_segval9",
		gcc.chart_of_accounts_id || '-' || nvl(gcc.segment10, '00') "coa_segval10",
		gcc.segment1,
		gcc.segment2,
		gcc.segment3,
		gcc.segment4,
		gcc.segment5,
		gcc.segment6,
		gcc.segment7,
		gcc.segment8,
		gcc.segment9,
		gcc.segment10,
		gb.period_net_cr,
		gb.period_net_dr,
		gb.quarter_to_date_cr,
		gb.quarter_to_date_dr,
		gb.project_to_date_cr,
		gb.project_to_date_dr,
		gb.begin_balance_cr,
		gb.begin_balance_dr
	from
		(select * from bec_ods.gl_balances where is_deleted_flg <> 'Y')  gb
	inner join (select * from bec_ods.gl_code_combinations where is_deleted_flg <> 'Y')  gcc
on
		gb.code_combination_id = gcc.code_combination_id
		where
		1 = 1
		and gcc.summary_flag = 'N'
		and gb.actual_flag = 'B'
		
			and  ( gb.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_bs_budget_activity'
				and batch_name = 'gl')
				OR
			gcc.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'fact_bs_budget_activity'
				and batch_name = 'gl')  )
) gbs
	where
		1 = 1
		and gbs.account_type in ('A', 'L', 'O')	
);
commit;
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_bs_budget_activity'
	and batch_name = 'gl';

COMMIT;