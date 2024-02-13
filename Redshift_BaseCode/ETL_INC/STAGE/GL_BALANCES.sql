/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
BEGIN;

TRUNCATE TABLE bec_ods_stg.GL_BALANCES;

insert into	bec_ods_stg.GL_BALANCES
    (ledger_id,
	code_combination_id,
	currency_code,
	period_name,
	actual_flag,
	last_update_date,
	last_updated_by,
	budget_version_id,
	encumbrance_type_id,
	translated_flag,
	revaluation_status,
	period_type,
	period_year,
	period_num,
	period_net_dr,
	period_net_cr,
	period_to_date_adb,
	quarter_to_date_dr,
	quarter_to_date_cr,
	quarter_to_date_adb,
	year_to_date_adb,
	project_to_date_dr,
	project_to_date_cr,
	project_to_date_adb,
	begin_balance_dr,
	begin_balance_cr,
	period_net_dr_beq,
	period_net_cr_beq,
	begin_balance_dr_beq,
	begin_balance_cr_beq,
	template_id,
	encumbrance_doc_id,
	encumbrance_line_num,
	quarter_to_date_dr_beq,
	quarter_to_date_cr_beq,
	project_to_date_dr_beq,
	project_to_date_cr_beq,
	KCA_OPERATION,
	KCA_SEQ_ID
	,kca_seq_date)
(select
	ledger_id,
	code_combination_id,
	currency_code,
	period_name,
	actual_flag,
	last_update_date,
	last_updated_by,
	budget_version_id,
	encumbrance_type_id,
	translated_flag,
	revaluation_status,
	period_type,
	period_year,
	period_num,
	period_net_dr,
	period_net_cr,
	period_to_date_adb,
	quarter_to_date_dr,
	quarter_to_date_cr,
	quarter_to_date_adb,
	year_to_date_adb,
	project_to_date_dr,
	project_to_date_cr,
	project_to_date_adb,
	begin_balance_dr,
	begin_balance_cr,
	period_net_dr_beq,
	period_net_cr_beq,
	begin_balance_dr_beq,
	begin_balance_cr_beq,
	template_id,
	encumbrance_doc_id,
	encumbrance_line_num,
	quarter_to_date_dr_beq,
	quarter_to_date_cr_beq,
	project_to_date_dr_beq,
	project_to_date_cr_beq,
	KCA_OPERATION,
	KCA_SEQ_ID
	,kca_seq_date
from
	bec_raw_dl_ext.GL_BALANCES 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
	and (LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE, PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID,0),nvl(ENCUMBRANCE_TYPE_ID,0),nvl(TRANSLATED_FLAG,'0'),KCA_SEQ_ID) in 
	(select LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE, PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID,0),nvl(ENCUMBRANCE_TYPE_ID,0),nvl(TRANSLATED_FLAG,'0'),max(KCA_SEQ_ID) from bec_raw_dl_ext.GL_BALANCES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE, PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID,0),nvl(ENCUMBRANCE_TYPE_ID,0),nvl(TRANSLATED_FLAG,'0'))
     and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='gl_balances')
	  
            )
	 );
END;