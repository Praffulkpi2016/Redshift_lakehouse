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

TRUNCATE TABLE bec_ods_stg.CST_QUANTITY_LAYERS;

insert into	bec_ods_stg.CST_QUANTITY_LAYERS
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
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.CST_QUANTITY_LAYERS 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(LAYER_ID,0),KCA_SEQ_ID) in 
	(select nvl(LAYER_ID,0) as LAYER_ID ,max(KCA_SEQ_ID) from bec_raw_dl_ext.CST_QUANTITY_LAYERS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(LAYER_ID,0))
     and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='cst_quantity_layers')
	 );
END;
 