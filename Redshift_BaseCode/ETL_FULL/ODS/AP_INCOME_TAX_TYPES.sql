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

DROP TABLE IF EXISTS bec_ods.ap_income_tax_types;

CREATE TABLE if NOT EXISTS bec_ods.ap_income_tax_types
(
	income_tax_type VARCHAR(10)   ENCODE lzo
	,description VARCHAR(80)   ENCODE lzo
	,inactive_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,ZD_EDITION_NAME VARCHAR(30)   ENCODE lzo
	,ZD_SYNC VARCHAR(30)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ap_income_tax_types (
    income_tax_type,
    description,
    inactive_date,
    last_update_date,
    last_updated_by,
    last_update_login,
    creation_date,
    created_by,
	zd_edition_name,
	zd_sync,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        income_tax_type,
        description,
        inactive_date,
        last_update_date,
        last_updated_by,
        last_update_login,
        creation_date,
        created_by,
		zd_edition_name,
		zd_sync,
	    KCA_OPERATION,
	    'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.ap_income_tax_types;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_income_tax_types';
	
commit;