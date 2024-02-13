/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
 
begin;
-- Delete Records

delete
from
	bec_ods.GL_BALANCES
where
	(
	LEDGER_ID,
	CODE_COMBINATION_ID,
	CURRENCY_CODE,
	PERIOD_NAME,
	ACTUAL_FLAG,
	nvl(BUDGET_VERSION_ID, 0),
	nvl(ENCUMBRANCE_TYPE_ID, 0),
	nvl(TRANSLATED_FLAG, '0')
	) in 
	(
	select
		stg.LEDGER_ID,
		stg.CODE_COMBINATION_ID,
		stg.CURRENCY_CODE,
		stg.PERIOD_NAME,
		stg.ACTUAL_FLAG,
		nvl(stg.BUDGET_VERSION_ID,0) as BUDGET_VERSION_ID,
		nvl(stg.ENCUMBRANCE_TYPE_ID,0) as ENCUMBRANCE_TYPE_ID,
		nvl(stg.TRANSLATED_FLAG,'0') as TRANSLATED_FLAG
	from
		bec_ods.GL_BALANCES ods,
		bec_ods_stg.GL_BALANCES stg
	where
	ods.LEDGER_ID = stg.LEDGER_ID
	and ods.CODE_COMBINATION_ID = stg.CODE_COMBINATION_ID
	and ods.CURRENCY_CODE = stg.CURRENCY_CODE
	and ods.PERIOD_NAME = stg.PERIOD_NAME
	and ods.ACTUAL_FLAG = stg.ACTUAL_FLAG
	and nvl(ods.BUDGET_VERSION_ID,0) = nvl(stg.BUDGET_VERSION_ID,0)
	and nvl(ods.ENCUMBRANCE_TYPE_ID,0) = nvl(stg.ENCUMBRANCE_TYPE_ID,0)
	and nvl(ods.TRANSLATED_FLAG,'0') = nvl(stg.TRANSLATED_FLAG,'0')
	and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.GL_BALANCES
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
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
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
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.GL_BALANCES
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		LEDGER_ID,
		CODE_COMBINATION_ID,
		CURRENCY_CODE,
		PERIOD_NAME,
		ACTUAL_FLAG,
		nvl(BUDGET_VERSION_ID, 0),
		nvl(ENCUMBRANCE_TYPE_ID, 0),
		nvl(TRANSLATED_FLAG, '0'),
		KCA_SEQ_ID
		) in 
	(
		select
			LEDGER_ID,
			CODE_COMBINATION_ID,
			CURRENCY_CODE,
			PERIOD_NAME,
			ACTUAL_FLAG,
			nvl(BUDGET_VERSION_ID, 0) as BUDGET_VERSION_ID,
			nvl(ENCUMBRANCE_TYPE_ID, 0) as ENCUMBRANCE_TYPE_ID,
			nvl(TRANSLATED_FLAG, '0') as TRANSLATED_FLAG,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.GL_BALANCES
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			LEDGER_ID,
			CODE_COMBINATION_ID,
			CURRENCY_CODE,
			PERIOD_NAME,
			ACTUAL_FLAG,
			nvl(BUDGET_VERSION_ID, 0),
			nvl(ENCUMBRANCE_TYPE_ID, 0),
			nvl(TRANSLATED_FLAG, '0')
			)	
	);

commit;

 
-- Soft delete
update bec_ods.GL_BALANCES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_BALANCES set IS_DELETED_FLG = 'Y'
where (LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE,PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID, 0),nvl(ENCUMBRANCE_TYPE_ID, 0),nvl(TRANSLATED_FLAG, '0'))  in
(
select LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE,PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID, 0),nvl(ENCUMBRANCE_TYPE_ID, 0),nvl(TRANSLATED_FLAG, '0') from bec_raw_dl_ext.GL_BALANCES
where (LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE,PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID, 0),nvl(ENCUMBRANCE_TYPE_ID, 0),nvl(TRANSLATED_FLAG, '0'),KCA_SEQ_ID)
in 
(
select LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE,PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID, 0),nvl(ENCUMBRANCE_TYPE_ID, 0),nvl(TRANSLATED_FLAG, '0'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.GL_BALANCES
group by LEDGER_ID,CODE_COMBINATION_ID,CURRENCY_CODE,PERIOD_NAME,ACTUAL_FLAG,nvl(BUDGET_VERSION_ID, 0),nvl(ENCUMBRANCE_TYPE_ID, 0),nvl(TRANSLATED_FLAG, '0')
) 
and kca_operation= 'DELETE'
);
commit;
end;
 

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'gl_balances';

commit;