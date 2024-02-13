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
drop table if exists bec_dwh.FACT_REV_EXP_TREND;
create table bec_dwh.fact_rev_exp_trend diststyle all sortkey(ledger_id,period_set_name,currency_code,period_name,code_combination_id)
as
(
select 
	ledger_id as ledger_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||ledger_id ledger_id_key,	
	period_set_name as period_set_name,
	chart_of_accounts_id as chart_of_accounts_id,
	currency_code as currency_code,
	period_name as period_name,
	period_name as gl_period,
	period_year as period_year,
	code_combination_id as code_combination_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| code_combination_id code_combination_id_key,	
	account_type as account_type,
	coa_segval3 as coa_segval3,
	coa_segval2 as coa_segval2,
	coa_segval1 as coa_segval1,
	coa_segval4 as coa_segval4,
	coa_segval5 as coa_segval5,
	coa_segval6 as coa_segval6,
	coa_segval7 as coa_segval7,
	coa_segval8 as coa_segval8,
	coa_segval9 as coa_segval9,
	coa_segval10 as coa_segval10,
	segment1 as segment1,
	segment2 as segment2,
	segment3 as segment3,
	segment4 as segment4,
	segment5 as segment5,
	segment6 as segment6,
	segment7 as segment7,
	segment8 as segment8,
	segment9 as segment9,
	segment10 as segment10,
	period_num,
	case
		when period_num = 1
		then (period_net_dr - period_net_cr)
		else 0
	end as period1_amount,
	case
		when period_num = 2
		then (period_net_dr - period_net_cr)
		else 0
	end as period2_amount,
	case
		when period_num = 3
		then (period_net_dr - period_net_cr)
		else 0
	end as period3_amount,
	case
		when period_num = 4
		then (period_net_dr - period_net_cr)
		else 0
	end as period4_amount,
	case
		when period_num = 5
		then (period_net_dr - period_net_cr)
		else 0
	end as period5_amount,
	case
		when period_num = 6
		then (period_net_dr - period_net_cr)
		else 0
	end as period6_amount,
	case
		when period_num = 7
		then (period_net_dr - period_net_cr)
		else 0
	end as period7_amount,
	case
		when period_num = 8
		then (period_net_dr - period_net_cr)
		else 0
	end as period8_amount,
	case
		when period_num = 9
		then (period_net_dr - period_net_cr)
		else 0
	end as period9_amount,
	case
		when period_num = 10
		then (period_net_dr - period_net_cr)
		else 0
	end as period10_amount,
	case
		when period_num = 11
		then (period_net_dr - period_net_cr)
		else 0
	end as period11_amount,
	case
		when period_num = 12
		then (period_net_dr - period_net_cr)
		else 0
	end as period12_amount,
	case
		when period_num in (1,2,3)
		then (period_net_dr - period_net_cr)
		else 0
	end as quarter1_amount,
	case
		when period_num in (4,5,6)
		then (period_net_dr - period_net_cr)
		else 0
	end as quarter2_amount,
	case
		when period_num in (7,8,9)
		then (period_net_dr - period_net_cr)
		else 0
	end as quarter3_amount,
	case
		when period_num in (10,11,12)
		then (period_net_dr - period_net_cr)
		else 0
	end as quarter4_amount,
	translated_flag,
	actual_flag,
	-- audit columns
	'N' AS IS_DELETED_FLG,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||
		nvl(ledger_id,0)||'-'||nvl(period_set_name,'NA')||'-'||nvl(currency_code,'NA')
		||'-'||upper(nvl(period_name,'NA'))||'-'||nvl(actual_flag,'X')||'-'||
		nvl(code_combination_id,0)||'-'||nvl(translated_flag,'X') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(
select 
	gb.ledger_id,
	gl.period_set_name,
	gb.currency_code,
	gb.period_name "period_name",
	gb.period_year,
	gb.actual_flag,
	gb.translated_flag,
	gb.budget_version_id,
	gcc.account_type,
	gcc.code_combination_id,
	gcc.chart_of_accounts_id,
	gb.period_num,
	gcc.chart_of_accounts_id ||'-'||nvl(gcc.segment1, '00') "coa_segval1",
	gcc.chart_of_accounts_id ||'-'||nvl(gcc.segment2, '00') "coa_segval2",
	gcc.chart_of_accounts_id ||'-'||nvl(gcc.segment3, '00') "coa_segval3",
	gcc.chart_of_accounts_id ||'-'||nvl(gcc.segment4, '00') "coa_segval4",
	gcc.chart_of_accounts_id ||'-'||nvl(gcc.segment5, '00') "coa_segval5",
	gcc.chart_of_accounts_id ||'-'||nvl(gcc.segment6, '00') "coa_segval6",
	gcc.chart_of_accounts_id ||'-'||nvl(gcc.segment7, '00') "coa_segval7",
	gcc.chart_of_accounts_id ||'-'||nvl(gcc.segment8, '00') "coa_segval8",
	gcc.chart_of_accounts_id ||'-'||nvl(gcc.segment9, '00') "coa_segval9",
	gcc.chart_of_accounts_id ||'-'||nvl(gcc.segment10, '00') "coa_segval10",
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
(select * from bec_ods.gl_balances where is_deleted_flg <> 'Y'and actual_flag = 'A') gb 
inner join (select * from bec_ods.gl_code_combinations where is_deleted_flg <> 'Y' and summary_flag = 'N' and account_type in ('R', 'E')) gcc 
on gb.code_combination_id = gcc.code_combination_id 
inner join (select period_set_name,ledger_id from bec_ods.gl_ledgers where is_deleted_flg <> 'Y') gl on gb.ledger_id = gl.ledger_id 
where 1 = 1 
) 
);
END;

UPDATE
	BEC_ETL_CTRL.BATCH_DW_INFO
SET
	LOAD_TYPE = 'I',
	LAST_REFRESH_DATE = GETDATE()
WHERE
	DW_TABLE_NAME = 'fact_rev_exp_trend'
	AND BATCH_NAME = 'gl';

COMMIT;