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

drop table if exists bec_dwh.FACT_XLA_REPORT_BALANCES_STG;

create table bec_dwh.FACT_XLA_REPORT_BALANCES_STG 
	diststyle all
	sortkey (LEDGER_ID,BUDGET_NAME)
as
SELECT
LEDGER_ID
,LEDGER_SHORT_NAME
,LEDGER_DESCRIPTION
,LEDGER_NAME
,LEDGER_CURRENCY
,PERIOD_YEAR
,PERIOD_NUMBER
,PERIOD_NAME
,PERIOD_START_DATE
,PERIOD_END_DATE
,BALANCE_TYPE_CODE
,BALANCE_TYPE
,BUDGET_VERSION_ID
,BUDGET_NAME
,ENCUMBRANCE_TYPE_ID
,ENCUMBRANCE_TYPE
,BEGIN_BALANCE_DR
,BEGIN_BALANCE_CR
,PERIOD_NET_DR
,PERIOD_NET_CR
,CODE_COMBINATION_ID
,CONTROL_ACCOUNT_FLAG
,CONTROL_ACCOUNT
,SEGMENT1
,SEGMENT2
,SEGMENT3
,SEGMENT4
,SEGMENT5
,SEGMENT6
,SEGMENT7
,SEGMENT8
,SEGMENT9
,SEGMENT10
,SEGMENT11
,SEGMENT12
,SEGMENT13
,SEGMENT14
,SEGMENT15
,SEGMENT16
,SEGMENT17
,SEGMENT18
,SEGMENT19
,SEGMENT20
,SEGMENT21
,SEGMENT22
,SEGMENT23
,SEGMENT24
,SEGMENT25
,SEGMENT26
,SEGMENT27
,SEGMENT28
,SEGMENT29
,SEGMENT30
,TRANSLATED_FLAG
,'N' AS IS_DELETED_FLG
,			(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id
,	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')
		|| '-' || nvl(LEDGER_ID, 0)
		|| '-' || nvl(BUDGET_NAME,'NA') 
		|| '-' || nvl(PERIOD_NAME,'NA') 
		|| '-' || nvl(CODE_COMBINATION_ID,0) 
		|| '-' || nvl(LEDGER_CURRENCY,'NA') 
		|| '-' || nvl(TRANSLATED_FLAG,'NA') 
		as dw_load_id
,getdate() as dw_insert_date
,getdate() as dw_update_date
from
(
SELECT 
            gl1.ledger_id                 LEDGER_ID
           ,gl1.short_name                LEDGER_SHORT_NAME
           ,gl1.description               LEDGER_DESCRIPTION
           ,gl1.NAME                      LEDGER_NAME
           ,glb.currency_code             LEDGER_CURRENCY
           ,glb.period_year               PERIOD_YEAR
           ,glb.period_num                PERIOD_NUMBER
           ,glb.period_name               PERIOD_NAME
           ,gl1.START_DATE                PERIOD_START_DATE
           ,gl1.end_date                  PERIOD_END_DATE
           ,glb.actual_flag               BALANCE_TYPE_CODE
           ,xlk.meaning                   BALANCE_TYPE
           ,glb.budget_version_id         BUDGET_VERSION_ID
           ,glv.budget_name               BUDGET_NAME
           ,glb.encumbrance_type_id       ENCUMBRANCE_TYPE_ID
           ,get.encumbrance_type          ENCUMBRANCE_TYPE
           ,NVL(glb.begin_balance_dr,0)   BEGIN_BALANCE_DR
           ,NVL(glb.begin_balance_cr,0)   BEGIN_BALANCE_CR
           ,NVL(glb.period_net_dr,0)      PERIOD_NET_DR
           ,NVL(glb.period_net_cr,0)      PERIOD_NET_CR
           ,glb.code_combination_id       CODE_COMBINATION_ID
--           ,xla_report_utility_pkg.get_ccid_desc
--              (gl1.chart_of_accounts_id
--              ,glb.code_combination_id)   CODE_COMBINATION_DESCRIPTION
           ,gcck.reference3               CONTROL_ACCOUNT_FLAG
           ,NULL                          CONTROL_ACCOUNT
           ,gcck.segment1                 SEGMENT1
           ,gcck.segment2                 SEGMENT2
           ,gcck.segment3                 SEGMENT3
           ,gcck.segment4                 SEGMENT4
           ,gcck.segment5                 SEGMENT5
           ,gcck.segment6                 SEGMENT6
           ,gcck.segment7                 SEGMENT7
           ,gcck.segment8                 SEGMENT8
           ,gcck.segment9                 SEGMENT9
           ,gcck.segment10                SEGMENT10
           ,gcck.segment11                SEGMENT11
           ,gcck.segment12                SEGMENT12
           ,gcck.segment13                SEGMENT13
           ,gcck.segment14                SEGMENT14
           ,gcck.segment15                SEGMENT15
           ,gcck.segment16                SEGMENT16
           ,gcck.segment17                SEGMENT17
           ,gcck.segment18                SEGMENT18
           ,gcck.segment19                SEGMENT19
           ,gcck.segment20                SEGMENT20
           ,gcck.segment21                SEGMENT21
           ,gcck.segment22                SEGMENT22
           ,gcck.segment23                SEGMENT23
           ,gcck.segment24                SEGMENT24
           ,gcck.segment25                SEGMENT25
           ,gcck.segment26                SEGMENT26
           ,gcck.segment27                SEGMENT27
           ,gcck.segment28                SEGMENT28
           ,gcck.segment29                SEGMENT29
           ,gcck.segment30                SEGMENT30
           ,glb.translated_flag TRANSLATED_FLAG
       FROM (SELECT 
                    gll.ledger_id
                   ,gll.short_name
                   ,gll.description
                   ,gll.name
                   ,gll.currency_code
                   ,gll.chart_of_accounts_id
                   ,gls.period_name
                   ,gls.start_date
                   ,gls.end_date
               FROM (select * from bec_ods.gl_ledgers where is_deleted_flg <> 'Y')                        gll
                   ,(select * from bec_ods.gl_period_statuses where is_deleted_flg <> 'Y')                gls
              WHERE gls.ledger_id              = gll.ledger_id
                AND gls.application_id         = 101
            )                                 gl1
           ,(select * from bec_ods.gl_balances where is_deleted_flg <> 'Y')                       glb
           ,(select * from bec_ods.gl_code_combinations where is_deleted_flg <> 'Y')              gcck
           ,(select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') xlk
           ,(select * from bec_ods.gl_budget_versions where is_deleted_flg <> 'Y')                glv
           ,(select * from bec_ods.gl_encumbrance_types where is_deleted_flg <> 'Y')              get
      WHERE glb.ledger_id              = gl1.ledger_id
        AND glb.period_name            = gl1.period_name
        AND glb.template_id            IS null
        AND gcck.code_combination_id   = glb.code_combination_id
	    AND gcck.chart_of_accounts_id  = gl1.chart_of_accounts_id
        AND xlk.lookup_type            = 'XLA_BALANCE_TYPE'
        AND xlk.lookup_code            = glb.actual_flag
        AND glv.budget_version_id(+)   = glb.budget_version_id
        AND get.encumbrance_type_id(+) = glb.encumbrance_type_id
);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_xla_report_balances_stg'
	and batch_name = 'gl';

commit;