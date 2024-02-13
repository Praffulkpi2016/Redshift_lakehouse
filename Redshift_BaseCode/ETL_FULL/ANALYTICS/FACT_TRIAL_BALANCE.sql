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
drop table if exists bec_dwh.FACT_TRIAL_BALANCE;

CREATE TABLE  bec_dwh.FACT_TRIAL_BALANCE 
	diststyle all sortkey(ledger_id,currency_code,period_set_name,period_name,actual_flag,code_combination_id) 
as 
( 
select 
     ledger_id,
	 period_set_name,
	 currency_code,
	 period_name,
	 gl_period,
	 period_year,
	 actual_flag,
	 budget_name,
	 account_type,
	 code_combination_id,
	 chart_of_accounts_id,
	 period_num,
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
	 period_net_cr,
	 period_net_dr,
	 quarter_to_date_cr,
	 quarter_to_date_dr,
	 project_to_date_cr,
	 project_to_date_dr,
	 begin_balance_cr,
	 begin_balance_dr,
	 amount,
	 amount_in_k,
	 ending_balance,
	 begin_balance,
	 translated_flag,
	 budget_version_id,
	'N' AS IS_DELETED_FLG,	 
		(
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
		)                   AS source_app_id,
		(
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
		) 
	   ||'-'||nvl(ledger_id,0) 
	   ||'-'|| nvl(period_set_name,'NA')	
       ||'-'||nvl(currency_code,'NA') 
	   ||'-'||nvl(period_name,'NA') 
	   ||'-'||nvl(actual_flag,'NA')		
	   ||'-'||nvl(code_combination_id,0) 
	   ||'-'||nvl(translated_flag,'x')
	   ||'-'||nvl(budget_version_id,0)	   AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
from
(   select  
	gb.ledger_id as ledger_id,
	gl.period_set_name as period_set_name,
	gb.currency_code as currency_code,
	upper(gb.period_name) as period_name,
	gb.period_name as gl_period,
	gb.period_year as period_year,
	gb.actual_flag as actual_flag,
	'ACTUAL' as budget_name,
	gcc.account_type as account_type,
	gcc.code_combination_id as code_combination_id,
	gcc.chart_of_accounts_id as chart_of_accounts_id,
	gb.period_num as period_num,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment1, '00') as coa_segval1,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment2, '00') as coa_segval2,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment3, '00') as coa_segval3,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment4, '00') as coa_segval4,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment5, '00') as coa_segval5,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment6, '00') as coa_segval6,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment7, '00') as coa_segval7,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment8, '00') as coa_segval8,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment9, '00') as coa_segval9,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment10, '00') as coa_segval10,
	gcc.segment1 as segment1,
	gcc.segment2 as segment2,
	gcc.segment3 as segment3,
	gcc.segment4 as segment4,
	gcc.segment5 as segment5,
	gcc.segment6 as segment6,
	gcc.segment7 as segment7,
	gcc.segment8 as segment8,
	gcc.segment9 as segment9,
	gcc.segment10 as segment10,
	gb.period_net_cr as period_net_cr,
	gb.period_net_dr as period_net_dr,
	gb.quarter_to_date_cr as quarter_to_date_cr,
	gb.quarter_to_date_dr as quarter_to_date_dr,
	gb.project_to_date_cr as project_to_date_cr,
	gb.project_to_date_dr as project_to_date_dr,
	gb.begin_balance_cr as begin_balance_cr,
	gb.begin_balance_dr as begin_balance_dr,
	(gb.period_net_dr - gb.period_net_cr) as amount,
	((gb.period_net_dr - gb.period_net_cr)/1000) as amount_in_k,
	--((gb.begin_balance_dr - gb.begin_balance_cr) - (gb.period_net_dr - gb.period_net_cr)) as ending_balance,
	--(gb.begin_balance_dr-gb.begin_balance_cr) - (gb.period_net_cr_beq-gb.period_net_dr_beq) Ending_Balance,
	(gb.begin_balance_dr-gb.begin_balance_cr)  + (gb.period_net_dr - gb.period_net_cr) Ending_Balance,
	(gb.begin_balance_dr-gb.begin_balance_cr) as begin_balance ,
	gb.translated_flag as translated_flag,
	gb.budget_version_id as  budget_version_id  
from (select * FROM bec_ods.gl_balances WHERE is_deleted_flg <> 'Y') gb
inner join (select * FROM bec_ods.gl_code_combinations WHERE is_deleted_flg <> 'Y') gcc
on
	gb.code_combination_id = gcc.code_combination_id
inner join (select * FROM bec_ods.gl_ledgers WHERE is_deleted_flg <> 'Y')  gl
on
	gb.ledger_id = gl.ledger_id
where 1=1
	and gb.actual_flag = 'A'
	and gcc.summary_flag = 'N'

union all

select  
	gb.ledger_id as ledger_id, 
	gl.period_set_name as period_set_name,
	gb.currency_code as currency_code,
	upper(gb.period_name) as period_name,
	gb.period_name as gl_period,
	gb.period_year as period_year,
	gb.actual_flag as actual_flag,
	gbv.budget_name as budget_name,
	gcc.account_type as account_type,
	gcc.code_combination_id as code_combination_id,
	gcc.chart_of_accounts_id as chart_of_accounts_id,
	gb.period_num as period_num,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment1, '00') as coa_segval1,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment2, '00') as coa_segval2,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment3, '00') as coa_segval3,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment4, '00') as coa_segval4,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment5, '00') as coa_segval5,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment6, '00') as coa_segval6,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment7, '00') as coa_segval7,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment8, '00') as coa_segval8,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment9, '00') as coa_segval9,
	gcc.chart_of_accounts_id||'-'||nvl(gcc.segment10, '00') as coa_segval10,
	gcc.segment1 as segment1,
	gcc.segment2 as segment2,
	gcc.segment3 as segment3,
	gcc.segment4 as segment4,
	gcc.segment5 as segment5,
	gcc.segment6 as segment6,
	gcc.segment7 as segment7,
	gcc.segment8 as segment8,
	gcc.segment9 as segment9,
	gcc.segment10 as segment10,
	gb.period_net_cr as period_net_cr,
	gb.period_net_dr as period_net_dr,
	gb.quarter_to_date_cr as quarter_to_date_cr,
	gb.quarter_to_date_dr as quarter_to_date_dr,
	gb.project_to_date_cr as project_to_date_cr,
	gb.project_to_date_dr as project_to_date_dr,
	gb.begin_balance_cr as begin_balance_cr,
	gb.begin_balance_dr as begin_balance_dr,
	(gb.period_net_dr - gb.period_net_cr) as amount,
	((gb.period_net_dr - gb.period_net_cr)/1000) as amount_in_k,
	--((gb.begin_balance_dr - gb.begin_balance_cr) - (gb.period_net_dr - gb.period_net_cr)) as ending_balance,
		--((gb.begin_balance_dr - gb.begin_balance_cr) - (gb.period_net_dr - gb.period_net_cr)) as ending_balance,
	--(gb.begin_balance_dr-gb.begin_balance_cr) - (gb.period_net_cr_beq-gb.period_net_dr_beq) Ending_Balance,
	(gb.begin_balance_dr-gb.begin_balance_cr)  + (gb.period_net_dr - gb.period_net_cr) Ending_Balance,
	(gb.begin_balance_dr - gb.begin_balance_cr) as begin_balance ,
	gb.translated_flag as translated_flag,
	gb.budget_version_id as  budget_version_id
from (SELECT * FROM bec_ods.gl_balances WHERE is_deleted_flg <> 'Y') gb
inner join (SELECT * FROM bec_ods.gl_code_combinations WHERE is_deleted_flg <> 'Y') gcc
on
	gb.code_combination_id = gcc.code_combination_id
inner join (SELECT * FROM bec_ods.gl_budget_versions WHERE is_deleted_flg <> 'Y')  gbv
on
	gbv.budget_version_id = gb.budget_version_id
inner join (SELECT * FROM  bec_ods.gl_ledgers WHERE is_deleted_flg <> 'Y') gl
on
	gb.ledger_id = gl.ledger_id
where 1=1
	and gb.actual_flag = 'B'
	and gcc.summary_flag = 'N'
)
); 
end; 
   
UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_trial_balance'
	and batch_name = 'gl';

commit;