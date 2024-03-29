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

truncate
	table bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS;

insert
	into
	bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS
    (lpn_id,
	license_plate_number,
	inventory_item_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	revision,
	lot_number,
	serial_number,
	organization_id,
	subinventory_code,
	locator_id,
	parent_lpn_id,
	gross_weight_uom_code,
	gross_weight,
	content_volume_uom_code,
	content_volume,
	tare_weight_uom_code,
	tare_weight,
	status_id,
	lpn_state,
	sealed_status,
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
	cost_group_id,
	lpn_context,
	lpn_reusability,
	outermost_lpn_id,
	homogeneous_container,
	source_type_id,
	source_header_id,
	source_line_id,
	source_line_detail_id,
	source_name,
	container_volume,
	container_volume_uom,
	catch_weight_flag,
	KCA_OPERATION,
	KCA_SEQ_ID
	,KCA_SEQ_DATE)
(
	select
    lpn_id,
	license_plate_number,
	inventory_item_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	revision,
	lot_number,
	serial_number,
	organization_id,
	subinventory_code,
	locator_id,
	parent_lpn_id,
	gross_weight_uom_code,
	gross_weight,
	content_volume_uom_code,
	content_volume,
	tare_weight_uom_code,
	tare_weight,
	status_id,
	lpn_state,
	sealed_status,
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
	cost_group_id,
	lpn_context,
	lpn_reusability,
	outermost_lpn_id,
	homogeneous_container,
	source_type_id,
	source_header_id,
	source_line_id,
	source_line_detail_id,
	source_name,
	container_volume,
	container_volume_uom,
	catch_weight_flag,
		KCA_OPERATION,
		KCA_SEQ_ID
		,KCA_SEQ_DATE
	from
		bec_raw_dl_ext.WMS_LICENSE_PLATE_NUMBERS
	where
		kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
		and (nvl(LPN_ID, 0), 
		KCA_SEQ_ID) in 
	(
		select
			nvl(LPN_ID, 0) as LPN_ID ,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.WMS_LICENSE_PLATE_NUMBERS
		where
			kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
		group by
			LPN_ID )
		and (KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wms_license_plate_numbers')
)
);
end;