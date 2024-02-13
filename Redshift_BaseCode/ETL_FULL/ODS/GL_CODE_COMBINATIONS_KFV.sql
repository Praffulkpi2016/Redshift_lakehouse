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

DROP TABLE if exists bec_ods.GL_CODE_COMBINATIONS_KFV;

CREATE TABLE IF NOT EXISTS bec_ods.GL_CODE_COMBINATIONS_KFV
(code_combination_id NUMERIC(15,0)   ENCODE az64
	,chart_of_accounts_id NUMERIC(15,0)   ENCODE az64
	,concatenated_segments VARCHAR(155)   ENCODE lzo
	,padded_concatenated_segments VARCHAR(74)   ENCODE lzo
	,gl_account_type VARCHAR(1)   ENCODE lzo
	,gl_control_account VARCHAR(25)   ENCODE lzo
	,reconciliation_flag VARCHAR(1)   ENCODE lzo
	,detail_budgeting_allowed VARCHAR(1)   ENCODE lzo
	,detail_posting_allowed VARCHAR(1)   ENCODE lzo
	,template_id NUMERIC(15,0)   ENCODE az64
	,ledger_segment VARCHAR(20)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,revaluation_id NUMERIC(15,0)   ENCODE az64
	,segment_attribute17 VARCHAR(60)   ENCODE lzo
	,segment_attribute18 VARCHAR(60)   ENCODE lzo
	,segment_attribute19 VARCHAR(60)   ENCODE lzo
	,segment_attribute20 VARCHAR(60)   ENCODE lzo
	,segment_attribute21 VARCHAR(60)   ENCODE lzo
	,segment_attribute22 VARCHAR(60)   ENCODE lzo
	,segment_attribute23 VARCHAR(60)   ENCODE lzo
	,segment_attribute24 VARCHAR(60)   ENCODE lzo
	,segment_attribute25 VARCHAR(60)   ENCODE lzo
	,segment_attribute26 VARCHAR(60)   ENCODE lzo
	,segment_attribute27 VARCHAR(60)   ENCODE lzo
	,segment_attribute28 VARCHAR(60)   ENCODE lzo
	,segment_attribute29 VARCHAR(60)   ENCODE lzo
	,segment_attribute30 VARCHAR(60)   ENCODE lzo
	,segment_attribute31 VARCHAR(60)   ENCODE lzo
	,segment_attribute32 VARCHAR(60)   ENCODE lzo
	,segment_attribute33 VARCHAR(60)   ENCODE lzo
	,segment_attribute34 VARCHAR(60)   ENCODE lzo
	,segment_attribute35 VARCHAR(60)   ENCODE lzo
	,segment_attribute36 VARCHAR(60)   ENCODE lzo
	,segment_attribute37 VARCHAR(60)   ENCODE lzo
	,segment_attribute38 VARCHAR(60)   ENCODE lzo
	,segment_attribute39 VARCHAR(60)   ENCODE lzo
	,segment_attribute40 VARCHAR(60)   ENCODE lzo
	,segment_attribute41 VARCHAR(60)   ENCODE lzo
	,segment_attribute42 VARCHAR(60)   ENCODE lzo
	,jgzz_recon_context VARCHAR(30)   ENCODE lzo
	,reference1 VARCHAR(1)   ENCODE lzo
	,reference2 VARCHAR(1)   ENCODE lzo
	,reference4 VARCHAR(1)   ENCODE lzo
	,reference5 VARCHAR(1)   ENCODE lzo
	,preserve_flag VARCHAR(1)   ENCODE lzo
	,refresh_flag VARCHAR(1)   ENCODE lzo
	,igi_balanced_budget_flag VARCHAR(1)   ENCODE lzo
	,company_cost_center_org_id NUMERIC(15,0)   ENCODE az64
	,alternate_code_combination_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,enabled_flag VARCHAR(1)   ENCODE lzo
	,summary_flag VARCHAR(1)   ENCODE lzo
	,segment1 VARCHAR(25)   ENCODE lzo
	,segment2 VARCHAR(25)   ENCODE lzo
	,segment3 VARCHAR(25)   ENCODE lzo
	,segment4 VARCHAR(25)   ENCODE lzo
	,segment5 VARCHAR(25)   ENCODE lzo
	,segment6 VARCHAR(25)   ENCODE lzo
	,segment7 VARCHAR(25)   ENCODE lzo
	,segment8 VARCHAR(25)   ENCODE lzo
	,segment9 VARCHAR(25)   ENCODE lzo
	,segment10 VARCHAR(25)   ENCODE lzo
	,segment11 VARCHAR(25)   ENCODE lzo
	,segment12 VARCHAR(25)   ENCODE lzo
	,segment13 VARCHAR(25)   ENCODE lzo
	,segment14 VARCHAR(25)   ENCODE lzo
	,segment15 VARCHAR(25)   ENCODE lzo
	,segment16 VARCHAR(25)   ENCODE lzo
	,segment17 VARCHAR(25)   ENCODE lzo
	,segment18 VARCHAR(25)   ENCODE lzo
	,segment19 VARCHAR(25)   ENCODE lzo
	,segment20 VARCHAR(25)   ENCODE lzo
	,segment21 VARCHAR(25)   ENCODE lzo
	,segment22 VARCHAR(25)   ENCODE lzo
	,segment23 VARCHAR(25)   ENCODE lzo
	,segment24 VARCHAR(25)   ENCODE lzo
	,segment25 VARCHAR(25)   ENCODE lzo
	,segment26 VARCHAR(25)   ENCODE lzo
	,segment27 VARCHAR(25)   ENCODE lzo
	,segment28 VARCHAR(25)   ENCODE lzo
	,segment29 VARCHAR(25)   ENCODE lzo
	,segment30 VARCHAR(25)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,allocation_create_flag VARCHAR(1)   ENCODE lzo
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,context VARCHAR(150)   ENCODE lzo
	,segment_attribute1 VARCHAR(60)   ENCODE lzo
	,segment_attribute2 VARCHAR(60)   ENCODE lzo
	,segment_attribute3 VARCHAR(60)   ENCODE lzo
	,segment_attribute4 VARCHAR(60)   ENCODE lzo
	,segment_attribute5 VARCHAR(60)   ENCODE lzo
	,segment_attribute6 VARCHAR(60)   ENCODE lzo
	,segment_attribute7 VARCHAR(60)   ENCODE lzo
	,segment_attribute8 VARCHAR(60)   ENCODE lzo
	,segment_attribute9 VARCHAR(60)   ENCODE lzo
	,segment_attribute10 VARCHAR(60)   ENCODE lzo
	,segment_attribute11 VARCHAR(60)   ENCODE lzo
	,segment_attribute12 VARCHAR(60)   ENCODE lzo
	,segment_attribute13 VARCHAR(60)   ENCODE lzo
	,segment_attribute14 VARCHAR(60)   ENCODE lzo
	,segment_attribute15 VARCHAR(60)   ENCODE lzo
	,segment_attribute16 VARCHAR(60)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64  ) 
DISTSTYLE AUTO
;


insert into bec_ods.GL_CODE_COMBINATIONS_KFV
(
code_combination_id,
chart_of_accounts_id,
concatenated_segments,
padded_concatenated_segments,
gl_account_type,
gl_control_account,
reconciliation_flag,
detail_budgeting_allowed,
detail_posting_allowed,
template_id,
ledger_segment,
attribute1,
revaluation_id,
segment_attribute17,
segment_attribute18,
segment_attribute19,
segment_attribute20,
segment_attribute21,
segment_attribute22,
segment_attribute23,
segment_attribute24,
segment_attribute25,
segment_attribute26,
segment_attribute27,
segment_attribute28,
segment_attribute29,
segment_attribute30,
segment_attribute31,
segment_attribute32,
segment_attribute33,
segment_attribute34,
segment_attribute35,
segment_attribute36,
segment_attribute37,
segment_attribute38,
segment_attribute39,
segment_attribute40,
segment_attribute41,
segment_attribute42,
jgzz_recon_context,
reference1,
reference2,
reference4,
reference5,
preserve_flag,
refresh_flag,
igi_balanced_budget_flag,
company_cost_center_org_id,
alternate_code_combination_id,
last_update_date,
last_updated_by,
enabled_flag,
summary_flag,
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
segment11,
segment12,
segment13,
segment14,
segment15,
segment16,
segment17,
segment18,
segment19,
segment20,
segment21,
segment22,
segment23,
segment24,
segment25,
segment26,
segment27,
segment28,
segment29,
segment30,
description,
allocation_create_flag,
start_date_active,
end_date_active,
attribute2,
attribute3,
attribute4,
attribute5,
attribute6,
attribute7,
attribute8,
attribute9,
attribute10,
context,
segment_attribute1,
segment_attribute2,
segment_attribute3,
segment_attribute4,
segment_attribute5,
segment_attribute6,
segment_attribute7,
segment_attribute8,
segment_attribute9,
segment_attribute10,
segment_attribute11,
segment_attribute12,
segment_attribute13,
segment_attribute14,
segment_attribute15,
segment_attribute16,
kca_operation 
,IS_DELETED_FLG,
kca_seq_id,
	kca_seq_date
)
SELECT
code_combination_id,
chart_of_accounts_id,
concatenated_segments,
padded_concatenated_segments,
gl_account_type,
gl_control_account,
reconciliation_flag,
detail_budgeting_allowed,
detail_posting_allowed,
template_id,
ledger_segment,
attribute1,
revaluation_id,
segment_attribute17,
segment_attribute18,
segment_attribute19,
segment_attribute20,
segment_attribute21,
segment_attribute22,
segment_attribute23,
segment_attribute24,
segment_attribute25,
segment_attribute26,
segment_attribute27,
segment_attribute28,
segment_attribute29,
segment_attribute30,
segment_attribute31,
segment_attribute32,
segment_attribute33,
segment_attribute34,
segment_attribute35,
segment_attribute36,
segment_attribute37,
segment_attribute38,
segment_attribute39,
segment_attribute40,
segment_attribute41,
segment_attribute42,
jgzz_recon_context,
reference1,
reference2,
reference4,
reference5,
preserve_flag,
refresh_flag,
igi_balanced_budget_flag,
company_cost_center_org_id,
alternate_code_combination_id,
last_update_date,
last_updated_by,
enabled_flag,
summary_flag,
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
segment11,
segment12,
segment13,
segment14,
segment15,
segment16,
segment17,
segment18,
segment19,
segment20,
segment21,
segment22,
segment23,
segment24,
segment25,
segment26,
segment27,
segment28,
segment29,
segment30,
description,
allocation_create_flag,
start_date_active,
end_date_active,
attribute2,
attribute3,
attribute4,
attribute5,
attribute6,
attribute7,
attribute8,
attribute9,
attribute10,
context,
segment_attribute1,
segment_attribute2,
segment_attribute3,
segment_attribute4,
segment_attribute5,
segment_attribute6,
segment_attribute7,
segment_attribute8,
segment_attribute9,
segment_attribute10,
segment_attribute11,
segment_attribute12,
segment_attribute13,
segment_attribute14,
segment_attribute15,
segment_attribute16,
kca_operation
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
FROM bec_ods_stg.GL_CODE_COMBINATIONS_KFV;

end;

update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'gl_code_combinations_kfv'; 
	
commit;