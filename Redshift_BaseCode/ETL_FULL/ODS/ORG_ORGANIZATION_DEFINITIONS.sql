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

DROP TABLE if exists bec_ods.ORG_ORGANIZATION_DEFINITIONS;

CREATE TABLE IF NOT EXISTS bec_ods.ORG_ORGANIZATION_DEFINITIONS
(
	organization_id NUMERIC(15,0)   ENCODE az64
	,business_group_id NUMERIC(15,0)   ENCODE az64
	,user_definition_enable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,organization_code VARCHAR(3)   ENCODE lzo
	,organization_name VARCHAR(240)   ENCODE lzo
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,chart_of_accounts_id NUMERIC(15,0)   ENCODE az64
	,inventory_enabled_flag VARCHAR(150)   ENCODE lzo
	,operating_unit NUMERIC(28,10)   ENCODE az64
	,legal_entity NUMERIC(28,10)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ORG_ORGANIZATION_DEFINITIONS (
    organization_id,
	business_group_id,
	user_definition_enable_date,
	disable_date,
	organization_code,
	organization_name,
	set_of_books_id,
	chart_of_accounts_id,
	inventory_enabled_flag,
	operating_unit,
	legal_entity,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
       organization_id,
	business_group_id,
	user_definition_enable_date,
	disable_date,
	organization_code,
	organization_name,
	set_of_books_id,
	chart_of_accounts_id,
	inventory_enabled_flag,
	operating_unit,
	legal_entity,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.ORG_ORGANIZATION_DEFINITIONS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'org_organization_definitions';
	
commit;