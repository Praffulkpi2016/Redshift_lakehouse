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

DROP TABLE if exists bec_ods.jtf_rs_resource_extns_tl;

CREATE TABLE IF NOT EXISTS bec_ods.jtf_rs_resource_extns_tl
(
	created_by NUMERIC(15,0)   ENCODE az64
	,resource_id NUMERIC(15,0)   ENCODE az64
	,category VARCHAR(30)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,"language" VARCHAR(4)   ENCODE lzo
	,resource_name VARCHAR(360)   ENCODE lzo
	,source_lang VARCHAR(4)   ENCODE lzo
	,security_group_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
	,is_deleted_flg VARCHAR(2)   ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
insert
	into
	bec_ods.jtf_rs_resource_extns_tl
(created_by,
	resource_id,
	category,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	"language",
	resource_name,
	source_lang,
	security_group_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	SELECT
	created_by,
	resource_id,
	category,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	"language",
	resource_name,
	source_lang,
	security_group_id,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.jtf_rs_resource_extns_tl;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'jtf_rs_resource_extns_tl';
	
commit;
	
