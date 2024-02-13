/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach to ODS.
# File Version: KPI v1.0
*/
begin;

TRUNCATE TABLE bec_ods.fnd_user_resp_groups_direct;

INSERT INTO bec_ods.fnd_user_resp_groups_direct (
		USER_ID,
	RESPONSIBILITY_ID,
	RESPONSIBILITY_APPLICATION_ID,
	SECURITY_GROUP_ID,
	START_DATE,
	END_DATE,
	CREATED_BY,
	CREATION_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATE_LOGIN,
	DESCRIPTION,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		USER_ID,
	RESPONSIBILITY_ID,
	RESPONSIBILITY_APPLICATION_ID,
	SECURITY_GROUP_ID,
	START_DATE,
	END_DATE,
	CREATED_BY,
	CREATION_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATE_LOGIN,
	DESCRIPTION,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.fnd_user_resp_groups_direct;

end;


UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fnd_user_resp_groups_direct';
commit;