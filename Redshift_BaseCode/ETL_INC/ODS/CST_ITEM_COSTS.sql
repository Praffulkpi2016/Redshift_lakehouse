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

delete from bec_ods.CST_ITEM_COSTS
where (COST_TYPE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID) in (
select stg.COST_TYPE_ID,stg.INVENTORY_ITEM_ID,stg.ORGANIZATION_ID
from bec_ods.CST_ITEM_COSTS ods, bec_ods_stg.CST_ITEM_COSTS stg
where ods.COST_TYPE_ID = stg.COST_TYPE_ID
and  ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
and ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.CST_ITEM_COSTS
       (inventory_item_id,
	organization_id,
	cost_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	inventory_asset_flag,
	lot_size,
	based_on_rollup_flag,
	shrinkage_rate,
	defaulted_flag,
	cost_update_id,
	pl_material,
	pl_material_overhead,
	pl_resource,
	pl_outside_processing,
	pl_overhead,
	tl_material,
	tl_material_overhead,
	tl_resource,
	tl_outside_processing,
	tl_overhead,
	material_cost,
	material_overhead_cost,
	resource_cost,
	outside_processing_cost,
	overhead_cost,
	pl_item_cost,
	tl_item_cost,
	item_cost,
	unburdened_cost,
	burden_cost,
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
	program_update_date,
	rollup_id,
	assignment_set_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)	
(
	select
		inventory_item_id,
	organization_id,
	cost_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	inventory_asset_flag,
	lot_size,
	based_on_rollup_flag,
	shrinkage_rate,
	defaulted_flag,
	cost_update_id,
	pl_material,
	pl_material_overhead,
	pl_resource,
	pl_outside_processing,
	pl_overhead,
	tl_material,
	tl_material_overhead,
	tl_resource,
	tl_outside_processing,
	tl_overhead,
	material_cost,
	material_overhead_cost,
	resource_cost,
	outside_processing_cost,
	overhead_cost,
	pl_item_cost,
	tl_item_cost,
	item_cost,
	unburdened_cost,
	burden_cost,
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
	program_update_date,
	rollup_id,
	assignment_set_id,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.CST_ITEM_COSTS
	where kca_operation IN ('INSERT','UPDATE') 
	and (COST_TYPE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,kca_seq_id) in 
	(select COST_TYPE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,max(kca_seq_id) from bec_ods_stg.CST_ITEM_COSTS 
     where kca_operation IN ('INSERT','UPDATE')
     group by COST_TYPE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID)
);

commit;

-- Soft delete
update bec_ods.CST_ITEM_COSTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CST_ITEM_COSTS set IS_DELETED_FLG = 'Y'
where (COST_TYPE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID)  in
(
select COST_TYPE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID from bec_raw_dl_ext.CST_ITEM_COSTS
where (COST_TYPE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,KCA_SEQ_ID)
in 
(
select COST_TYPE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CST_ITEM_COSTS
group by COST_TYPE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'cst_item_costs';

commit;