/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
BEGIN;

TRUNCATE TABLE bec_ods_stg.CST_COST_TYPES;

insert into	bec_ods_stg.CST_COST_TYPES
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
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.CST_COST_TYPES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(COST_TYPE_ID,0),KCA_SEQ_ID) in 
	(select nvl(COST_TYPE_ID,0) as COST_TYPE_ID ,max(KCA_SEQ_ID) from bec_raw_dl_ext.CST_COST_TYPES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(COST_TYPE_ID,0))
     and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='cst_cost_types')
	 );
END;