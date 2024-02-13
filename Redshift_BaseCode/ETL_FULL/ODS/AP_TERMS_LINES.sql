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

drop table if exists bec_ods.ap_terms_lines;

CREATE TABLE IF NOT EXISTS bec_ods.ap_terms_lines
(
	term_id NUMERIC(15,0)   ENCODE az64
	,sequence_num NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,due_percent NUMERIC(28,10)   ENCODE az64
	,due_amount NUMERIC(28,10)   ENCODE az64
	,due_days NUMERIC(15,0)   ENCODE az64
	,due_day_of_month NUMERIC(15,0)   ENCODE az64
	,due_months_forward NUMERIC(15,0)   ENCODE az64
	,discount_percent NUMERIC(28,10)   ENCODE az64
	,discount_days NUMERIC(15,0)   ENCODE az64
	,discount_day_of_month NUMERIC(15,0)   ENCODE az64
	,discount_months_forward NUMERIC(15,0)   ENCODE az64
	,discount_percent_2 NUMERIC(28,10)   ENCODE az64
	,discount_days_2 NUMERIC(15,0)   ENCODE az64
	,discount_day_of_month_2 NUMERIC(15,0)   ENCODE az64
	,discount_months_forward_2 NUMERIC(15,0)   ENCODE az64
	,discount_percent_3 NUMERIC(28,10)   ENCODE az64
	,discount_days_3 NUMERIC(15,0)   ENCODE az64
	,discount_day_of_month_3 NUMERIC(15,0)   ENCODE az64
	,discount_months_forward_3 NUMERIC(15,0)   ENCODE az64
	,attribute_category VARCHAR(150)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,fixed_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,calendar VARCHAR(30)   ENCODE lzo
	,discount_amount NUMERIC(28,10)   ENCODE az64
	,discount_criteria VARCHAR(1)   ENCODE lzo
	,discount_amount_2 NUMERIC(28,10)   ENCODE az64
	,discount_criteria_2 VARCHAR(1)   ENCODE lzo
	,discount_amount_3 NUMERIC(28,10)   ENCODE az64
	,discount_criteria_3 VARCHAR(1)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO;

insert
	into
	bec_ods.ap_terms_lines
(term_id,
	sequence_num,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	due_percent,
	due_amount,
	due_days,
	due_day_of_month,
	due_months_forward,
	discount_percent,
	discount_days,
	discount_day_of_month,
	discount_months_forward,
	discount_percent_2,
	discount_days_2,
	discount_day_of_month_2,
	discount_months_forward_2,
	discount_percent_3,
	discount_days_3,
	discount_day_of_month_3,
	discount_months_forward_3,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	fixed_date,
	calendar,
	discount_amount,
	discount_criteria,
	discount_amount_2,
	discount_criteria_2,
	discount_amount_3,
	discount_criteria_3
	,KCA_OPERATION 
	,IS_DELETED_FLG
	,kca_seq_id
	,kca_seq_date	)

 select
	term_id,
	sequence_num,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	due_percent,
	due_amount,
	due_days,
	due_day_of_month,
	due_months_forward,
	discount_percent,
	discount_days,
	discount_day_of_month,
	discount_months_forward,
	discount_percent_2,
	discount_days_2,
	discount_day_of_month_2,
	discount_months_forward_2,
	discount_percent_3,
	discount_days_3,
	discount_day_of_month_3,
	discount_months_forward_3,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	fixed_date,
	calendar,
	discount_amount,
	discount_criteria,
	discount_amount_2,
	discount_criteria_2,
	discount_amount_3,
	discount_criteria_3,
	KCA_OPERATION 
   ,'N' as IS_DELETED_FLG
	,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
from
	bec_ods_stg.ap_terms_lines;	
end;

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_terms_lines';
	
commit;


