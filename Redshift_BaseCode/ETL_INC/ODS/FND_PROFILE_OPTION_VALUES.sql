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

truncate table bec_ods.FND_PROFILE_OPTION_VALUES;

INSERT INTO bec_ods.FND_PROFILE_OPTION_VALUES (
	 application_id 
    ,profile_option_id 
    ,level_id
    ,level_value 
    ,last_update_date 
    ,last_updated_by 
    ,creation_date 
    ,created_by 
    ,last_update_login 
    ,profile_option_value 
    ,level_value_application_id 
    ,level_value2 
    ,zd_edition_name 
    ,zd_sync, 
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	 application_id 
    ,profile_option_id 
    ,level_id
    ,level_value 
    ,last_update_date 
    ,last_updated_by 
    ,creation_date 
    ,created_by 
    ,last_update_login 
    ,profile_option_value 
    ,level_value_application_id 
    ,level_value2 
    ,zd_edition_name 
    ,zd_sync, 
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.FND_PROFILE_OPTION_VALUES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fnd_profile_option_values';
	
commit;