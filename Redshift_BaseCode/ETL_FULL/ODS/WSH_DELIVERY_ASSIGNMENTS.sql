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

DROP TABLE if exists bec_ods.WSH_DELIVERY_ASSIGNMENTS;

CREATE TABLE IF NOT EXISTS bec_ods.WSH_DELIVERY_ASSIGNMENTS
(

    delivery_assignment_id NUMERIC(15,0)   ENCODE az64
	,delivery_id NUMERIC(15,0)   ENCODE az64
	,parent_delivery_id NUMERIC(15,0)   ENCODE az64
	,delivery_detail_id NUMERIC(15,0)   ENCODE az64
	,parent_delivery_detail_id NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,active_flag VARCHAR(1)   ENCODE lzo
	,"type" VARCHAR(30)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

insert
	into
	bec_ods.WSH_DELIVERY_ASSIGNMENTS (
    delivery_assignment_id,
	delivery_id,
	parent_delivery_id,
	delivery_detail_id,
	parent_delivery_detail_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	active_flag,
	"type",
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    select
	delivery_assignment_id,
	delivery_id,
	parent_delivery_id,
	delivery_detail_id,
	parent_delivery_detail_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	active_flag,
	"type",
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date
from
	bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'wsh_delivery_assignments';

commit;