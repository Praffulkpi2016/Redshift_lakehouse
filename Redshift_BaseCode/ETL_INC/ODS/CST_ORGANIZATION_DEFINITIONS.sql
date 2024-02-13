/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for ODS.
# File Version: KPI v1.0
*/

begin;

TRUNCATE TABLE bec_ods.CST_ORGANIZATION_DEFINITIONS;

INSERT INTO bec_ods.CST_ORGANIZATION_DEFINITIONS (
	organization_id,
	business_group_id,
	user_definition_enable_date,
	disable_date,
	organization_code,
	organization_name,
	set_of_books_id,
	chart_of_accounts_id,
	currency_code,
	operating_unit,
	legal_entity,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
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
	currency_code,
	operating_unit,
	legal_entity,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.CST_ORGANIZATION_DEFINITIONS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cst_organization_definitions';
	
commit;