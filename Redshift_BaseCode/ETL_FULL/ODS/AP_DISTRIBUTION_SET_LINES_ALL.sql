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

DROP TABLE IF EXISTS bec_ods.ap_distribution_set_lines_all;

CREATE TABLE if NOT EXISTS bec_ods.ap_distribution_set_lines_all
(
	distribution_set_id NUMERIC(15,0)   ENCODE az64
	,dist_code_combination_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,percent_distribution NUMERIC(28,10)   ENCODE az64
	,type_1099 VARCHAR(10)   ENCODE lzo
	,vat_code VARCHAR(15)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,distribution_set_line_number NUMERIC(15,0)   ENCODE az64
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
	,project_accounting_context VARCHAR(30)   ENCODE lzo
	,task_id NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,expenditure_organization_id NUMERIC(15,0)   ENCODE az64
	,expenditure_type VARCHAR(30)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,award_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ap_distribution_set_lines_all 
    (distribution_set_id,
    dist_code_combination_id,
    last_update_date,
    last_updated_by,
    set_of_books_id,
    percent_distribution,
    type_1099,
    vat_code,
    description,
    last_update_login,
    creation_date,
    created_by,
    distribution_set_line_number,
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
    project_accounting_context,
    task_id,
    project_id,
    expenditure_organization_id,
    expenditure_type,
    org_id,
    award_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	KCA_SEQ_ID,
	kca_seq_date
)
    SELECT
        distribution_set_id,
        dist_code_combination_id,
        last_update_date,
        last_updated_by,
        set_of_books_id,
        percent_distribution,
        type_1099,
        vat_code,
        description,
        last_update_login,
        creation_date,
        created_by,
        distribution_set_line_number,
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
        project_accounting_context,
        task_id,
        project_id,
        expenditure_organization_id,
        expenditure_type,
        org_id,
        award_id,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.ap_distribution_set_lines_all;

end;	

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_distribution_set_lines_all';
	
commit;