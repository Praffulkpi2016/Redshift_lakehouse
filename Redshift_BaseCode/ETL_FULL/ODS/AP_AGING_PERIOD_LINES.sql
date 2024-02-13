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

DROP TABLE if exists bec_ods.ap_aging_period_lines;

CREATE TABLE IF NOT EXISTS bec_ods.ap_aging_period_lines
(
	aging_period_line_id NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)  ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,aging_period_id NUMERIC(15,0)   ENCODE az64
	,period_sequence_num NUMERIC(15,0)   ENCODE az64
	,days_start NUMERIC(15,0)   ENCODE az64
	,days_to NUMERIC(15,0)   ENCODE az64
	,type VARCHAR(25)   ENCODE lzo
	,report_heading1 VARCHAR(15)   ENCODE lzo
	,report_heading2 VARCHAR(15)   ENCODE lzo
	,report_heading3 VARCHAR(15)   ENCODE lzo
	,new_line VARCHAR(30)   ENCODE lzo
	,base_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ap_aging_period_lines (
    aging_period_line_id,
    last_updated_by,
    last_update_date,
    created_by,
    creation_date,
    aging_period_id,
    period_sequence_num,
    days_start,
    days_to,
    type,
    report_heading1,
    report_heading2,
    report_heading3,
    new_line,
    base_date,
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
    last_update_login,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        aging_period_line_id,
        last_updated_by,
        last_update_date,
        created_by,
        creation_date,
        aging_period_id,
        period_sequence_num,
        days_start,
        days_to,
        type,
        report_heading1,
        report_heading2,
        report_heading3,
        new_line,
        base_date,
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
        last_update_login,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.ap_aging_period_lines;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_aging_period_lines';
	
commit;