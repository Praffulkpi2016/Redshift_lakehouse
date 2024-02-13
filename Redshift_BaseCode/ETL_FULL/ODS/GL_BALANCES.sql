/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.GL_BALANCES;

CREATE TABLE IF NOT EXISTS bec_ods.GL_BALANCES
(
	 ledger_id NUMERIC(15,0)   ENCODE az64
	,code_combination_id NUMERIC(15,0)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,period_name VARCHAR(15)   ENCODE lzo
	,actual_flag VARCHAR(1)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,budget_version_id NUMERIC(15,0)   ENCODE az64
	,encumbrance_type_id NUMERIC(15,0)   ENCODE az64
	,translated_flag VARCHAR(1)   ENCODE lzo
	,revaluation_status VARCHAR(1)   ENCODE lzo
	,period_type VARCHAR(15)   ENCODE lzo
	,period_year NUMERIC(15,0)   ENCODE az64
	,period_num NUMERIC(15,0)   ENCODE az64
	,period_net_dr NUMERIC(28,10)   ENCODE az64
	,period_net_cr NUMERIC(28,10)   ENCODE az64
	,period_to_date_adb NUMERIC(28,10)   ENCODE az64
	,quarter_to_date_dr NUMERIC(28,10)   ENCODE az64
	,quarter_to_date_cr NUMERIC(28,10)   ENCODE az64
	,quarter_to_date_adb NUMERIC(28,10)   ENCODE az64
	,year_to_date_adb NUMERIC(28,10)   ENCODE az64
	,project_to_date_dr NUMERIC(28,10)   ENCODE az64
	,project_to_date_cr NUMERIC(28,10)   ENCODE az64
	,project_to_date_adb NUMERIC(28,10)   ENCODE az64
	,begin_balance_dr NUMERIC(28,10)   ENCODE az64
	,begin_balance_cr NUMERIC(28,10)   ENCODE az64
	,period_net_dr_beq NUMERIC(28,10)   ENCODE az64
	,period_net_cr_beq NUMERIC(28,10)   ENCODE az64
	,begin_balance_dr_beq NUMERIC(28,10)   ENCODE az64
	,begin_balance_cr_beq NUMERIC(28,10)   ENCODE az64
	,template_id NUMERIC(15,0)   ENCODE az64
	,encumbrance_doc_id NUMERIC(15,0)   ENCODE az64
	,encumbrance_line_num NUMERIC(15,0)   ENCODE az64
	,quarter_to_date_dr_beq NUMERIC(28,10)   ENCODE az64
	,quarter_to_date_cr_beq NUMERIC(28,10)   ENCODE az64
	,project_to_date_dr_beq NUMERIC(28,10)   ENCODE az64
	,project_to_date_cr_beq NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64  ) 
DISTSTYLE
auto;

INSERT INTO bec_ods.GL_BALANCES (
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
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
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
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.GL_BALANCES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'gl_balances';
	
commit;