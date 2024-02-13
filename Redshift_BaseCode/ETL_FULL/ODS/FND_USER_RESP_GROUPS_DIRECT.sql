/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach to ODS.
# File Version: KPI v1.0
*/


begin;

DROP TABLE if exists bec_ods.fnd_user_resp_groups_direct;

CREATE TABLE IF NOT EXISTS bec_ods.fnd_user_resp_groups_direct
(
	USER_ID NUMERIC(15,0)   ENCODE az64
	,RESPONSIBILITY_ID NUMERIC(15,0)  ENCODE az64
	,RESPONSIBILITY_APPLICATION_ID NUMERIC(15,0)  ENCODE az64
	,SECURITY_GROUP_ID NUMERIC(15,0)   ENCODE az64
	,COMPONENT_ITEM_ID NUMERIC(15,0)  ENCODE az64
	,START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,END_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CREATED_BY NUMERIC(15,0)   ENCODE az64
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64
	,DESCRIPTION VARCHAR(2000)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64  ) 
DISTSTYLE
auto;

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
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.fnd_user_resp_groups_direct;

end;


UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fnd_user_resp_groups_direct';