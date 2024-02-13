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
begin;

truncate table bec_ods_stg.MTL_UOM_CONVERSIONS;

insert into	bec_ods_stg.MTL_UOM_CONVERSIONS
   (unit_of_measure,
	uom_code,
	uom_class,
	inventory_item_id,
	conversion_rate,
	default_conversion_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	disable_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	length,
	width,
	height,
	dimension_uom,
	kca_operation,
	kca_seq_id
	,kca_seq_date)
(
	select
		unit_of_measure,
	uom_code,
	uom_class,
	inventory_item_id,
	conversion_rate,
	default_conversion_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	disable_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	length,
	width,
	height,
	dimension_uom,
	kca_operation,
	kca_seq_id
	,kca_seq_date
	from bec_raw_dl_ext.MTL_UOM_CONVERSIONS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
	and (INVENTORY_ITEM_ID,UNIT_OF_MEASURE,kca_seq_id) in 
	(select INVENTORY_ITEM_ID,UNIT_OF_MEASURE,max(kca_seq_id) from bec_raw_dl_ext.MTL_UOM_CONVERSIONS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by INVENTORY_ITEM_ID,UNIT_OF_MEASURE)
        and	(kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_uom_conversions')
            )	
			
);
end;