/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
 
begin;
 
-- Delete Records

delete from bec_ods.CST_COST_TYPES
where (nvl(COST_TYPE_ID,0)) in (
select nvl(stg.COST_TYPE_ID,0) as COST_TYPE_ID
from bec_ods.CST_COST_TYPES ods, bec_ods_stg.CST_COST_TYPES stg
where nvl(ods.COST_TYPE_ID,0) = nvl(stg.COST_TYPE_ID,0) AND
 stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.CST_COST_TYPES
    (cost_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	organization_id,
	cost_type,
	description,
	costing_method_type,
	frozen_standard_flag,
	default_cost_type_id,
	bom_snapshot_flag,
	alternate_bom_designator,
	allow_updates_flag,
	pl_element_flag,
	pl_resource_flag,
	pl_operation_flag,
	pl_activity_flag,
	disable_date,
	available_to_eng_flag,
	component_yield_flag,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date
	,ZD_EDITION_NAME
	,ZD_SYNC
	,KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	cost_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	organization_id,
	cost_type,
	description,
	costing_method_type,
	frozen_standard_flag,
	default_cost_type_id,
	bom_snapshot_flag,
	alternate_bom_designator,
	allow_updates_flag,
	pl_element_flag,
	pl_resource_flag,
	pl_operation_flag,
	pl_activity_flag,
	disable_date,
	available_to_eng_flag,
	component_yield_flag,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date
	,ZD_EDITION_NAME
	,ZD_SYNC
	,KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.CST_COST_TYPES
where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(COST_TYPE_ID,0),KCA_SEQ_ID) in 
	(select nvl(COST_TYPE_ID,0) AS COST_TYPE_ID,max(KCA_SEQ_ID) from bec_ods_stg.CST_COST_TYPES 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(COST_TYPE_ID,0))	
	);

commit;

-- Soft delete
update bec_ods.CST_COST_TYPES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CST_COST_TYPES set IS_DELETED_FLG = 'Y'
where (COST_TYPE_ID)  in
(
select COST_TYPE_ID from bec_raw_dl_ext.CST_COST_TYPES
where (COST_TYPE_ID,KCA_SEQ_ID)
in 
(
select COST_TYPE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CST_COST_TYPES
group by COST_TYPE_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;
 

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'cst_cost_types';

commit;