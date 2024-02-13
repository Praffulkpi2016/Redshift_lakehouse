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

delete from bec_ods.CST_QUANTITY_LAYERS
where (nvl(LAYER_ID,0) ) in (
select nvl(stg.LAYER_ID,0) AS LAYER_ID  
from bec_ods.CST_QUANTITY_LAYERS ods, bec_ods_stg.CST_QUANTITY_LAYERS stg
where nvl(ods.LAYER_ID,0) = nvl(stg.LAYER_ID,0) AND 
      stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.CST_QUANTITY_LAYERS
    (layer_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	organization_id,
	inventory_item_id,
	layer_quantity,
	create_transaction_id,
	update_transaction_id,
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
	cost_group_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	layer_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	organization_id,
	inventory_item_id,
	layer_quantity,
	create_transaction_id,
	update_transaction_id,
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
	cost_group_id,
	KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.CST_QUANTITY_LAYERS
where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(LAYER_ID,0),KCA_SEQ_ID) in 
	(select nvl(LAYER_ID,0) AS LAYER_ID,max(KCA_SEQ_ID) from bec_ods_stg.CST_QUANTITY_LAYERS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(LAYER_ID,0))	
	);

commit;

-- Soft delete
update bec_ods.CST_QUANTITY_LAYERS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CST_QUANTITY_LAYERS set IS_DELETED_FLG = 'Y'
where (LAYER_ID)  in
(
select LAYER_ID from bec_raw_dl_ext.CST_QUANTITY_LAYERS
where (LAYER_ID,KCA_SEQ_ID)
in 
(
select LAYER_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CST_QUANTITY_LAYERS
group by LAYER_ID
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
	ods_table_name = 'cst_quantity_layers';

commit;