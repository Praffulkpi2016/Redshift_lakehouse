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

DROP TABLE if exists bec_ods.WIP_PERIOD_BALANCES;

CREATE TABLE IF NOT EXISTS bec_ods.WIP_PERIOD_BALANCES
(
	acct_period_id NUMERIC(15,0)   ENCODE az64
	,wip_entity_id NUMERIC(15,0)   ENCODE az64
	,repetitive_schedule_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,class_type NUMERIC(15,0)   ENCODE az64
	,tl_resource_in NUMERIC(28,10)   ENCODE az64
	,tl_overhead_in NUMERIC(28,10)   ENCODE az64
	,tl_outside_processing_in NUMERIC(28,10)   ENCODE az64
	,pl_material_in NUMERIC(28,10)   ENCODE az64
	,pl_material_overhead_in NUMERIC(28,10)   ENCODE az64
	,pl_resource_in NUMERIC(28,10)   ENCODE az64
	,pl_overhead_in NUMERIC(28,10)   ENCODE az64
	,pl_outside_processing_in NUMERIC(28,10)   ENCODE az64
	,tl_material_out NUMERIC(28,10)   ENCODE az64
	,tl_material_overhead_out NUMERIC(28,10)   ENCODE az64
	,tl_resource_out NUMERIC(28,10)   ENCODE az64
	,tl_overhead_out NUMERIC(28,10)   ENCODE az64
	,tl_outside_processing_out NUMERIC(28,10)   ENCODE az64
	,pl_material_out NUMERIC(28,10)   ENCODE az64
	,pl_material_overhead_out NUMERIC(28,10)   ENCODE az64
	,pl_resource_out NUMERIC(28,10)   ENCODE az64
	,pl_overhead_out NUMERIC(28,10)   ENCODE az64
	,pl_outside_processing_out NUMERIC(28,10)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,tl_material_var NUMERIC(28,10)   ENCODE az64
	,tl_material_overhead_var NUMERIC(28,10)   ENCODE az64
	,tl_resource_var NUMERIC(28,10)   ENCODE az64
	,tl_outside_processing_var NUMERIC(28,10)   ENCODE az64
	,tl_overhead_var NUMERIC(28,10)   ENCODE az64
	,pl_material_var NUMERIC(28,10)   ENCODE az64
	,pl_material_overhead_var NUMERIC(28,10)   ENCODE az64
	,pl_resource_var NUMERIC(28,10)   ENCODE az64
	,pl_overhead_var NUMERIC(28,10)   ENCODE az64
	,pl_outside_processing_var NUMERIC(28,10)   ENCODE az64
	,tl_scrap_in NUMERIC(28,10)   ENCODE az64
	,tl_scrap_out NUMERIC(28,10)   ENCODE az64
	,tl_scrap_var NUMERIC(28,10)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.WIP_PERIOD_BALANCES (
   acct_period_id,
	wip_entity_id,
	repetitive_schedule_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	organization_id,
	class_type,
	tl_resource_in,
	tl_overhead_in,
	tl_outside_processing_in,
	pl_material_in,
	pl_material_overhead_in,
	pl_resource_in,
	pl_overhead_in,
	pl_outside_processing_in,
	tl_material_out,
	tl_material_overhead_out,
	tl_resource_out,
	tl_overhead_out,
	tl_outside_processing_out,
	pl_material_out,
	pl_material_overhead_out,
	pl_resource_out,
	pl_overhead_out,
	pl_outside_processing_out,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	tl_material_var,
	tl_material_overhead_var,
	tl_resource_var,
	tl_outside_processing_var,
	tl_overhead_var,
	pl_material_var,
	pl_material_overhead_var,
	pl_resource_var,
	pl_overhead_var,
	pl_outside_processing_var,
	tl_scrap_in,
	tl_scrap_out,
	tl_scrap_var,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date 
)
    SELECT
        acct_period_id,
	wip_entity_id,
	repetitive_schedule_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	organization_id,
	class_type,
	tl_resource_in,
	tl_overhead_in,
	tl_outside_processing_in,
	pl_material_in,
	pl_material_overhead_in,
	pl_resource_in,
	pl_overhead_in,
	pl_outside_processing_in,
	tl_material_out,
	tl_material_overhead_out,
	tl_resource_out,
	tl_overhead_out,
	tl_outside_processing_out,
	pl_material_out,
	pl_material_overhead_out,
	pl_resource_out,
	pl_overhead_out,
	pl_outside_processing_out,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	tl_material_var,
	tl_material_overhead_var,
	tl_resource_var,
	tl_outside_processing_var,
	tl_overhead_var,
	pl_material_var,
	pl_material_overhead_var,
	pl_resource_var,
	pl_overhead_var,
	pl_outside_processing_var,
	tl_scrap_in,
	tl_scrap_out,
	tl_scrap_var,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.WIP_PERIOD_BALANCES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wip_period_balances';
	
COMMIT;