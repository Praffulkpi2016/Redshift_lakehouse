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

DROP TABLE if exists bec_ods.RA_TERMS_B;

CREATE TABLE IF NOT EXISTS bec_ods.RA_TERMS_B
(
    term_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,credit_check_flag VARCHAR(1)   ENCODE lzo
	,due_cutoff_day NUMERIC(28,10)   ENCODE az64
	,printing_lead_days NUMERIC(28,10)   ENCODE az64
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
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
	,base_amount NUMERIC(28,10)   ENCODE az64
	,calc_discount_on_lines_flag VARCHAR(1)   ENCODE lzo
	,first_installment_code VARCHAR(12)   ENCODE lzo
	,in_use VARCHAR(1)   ENCODE lzo
	,partial_discount_flag VARCHAR(1)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,prepayment_flag VARCHAR(1)   ENCODE lzo
	,billing_cycle_id NUMERIC(15,0)   ENCODE az64
	,zd_edition_name VARCHAR(30)  ENCODE lzo
	,zd_sync VARCHAR(30)  ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.RA_TERMS_B (
	term_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	credit_check_flag,
	due_cutoff_day,
	printing_lead_days,
	start_date_active,
	end_date_active,
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
	base_amount,
	calc_discount_on_lines_flag,
	first_installment_code,
	in_use,
	partial_discount_flag,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	prepayment_flag,
	billing_cycle_id,
	zd_edition_name,
	zd_sync,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
     	term_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login, 
	credit_check_flag,
	due_cutoff_day,
	printing_lead_days, 
	start_date_active,
	end_date_active,
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
	base_amount,
	calc_discount_on_lines_flag,
	first_installment_code,
	in_use,
	partial_discount_flag,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	prepayment_flag,
	billing_cycle_id,
	zd_edition_name,
	zd_sync,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.RA_TERMS_B;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ra_terms_b';
	
COMMIT;