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

delete
from
	bec_ods.WMS_LICENSE_PLATE_NUMBERS
where
	(
	nvl(LPN_ID, 0) 
	) in 
	(
	select
		NVL(stg.LPN_ID, 0) as LPN_ID 
	from
		bec_ods.WMS_LICENSE_PLATE_NUMBERS ods,
		bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS stg
	where
		    NVL(ods.LPN_ID, 0) = NVL(stg.LPN_ID, 0) 
					and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.WMS_LICENSE_PLATE_NUMBERS (
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
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date)
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date		
	from
		bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS
	where
		kca_operation in ('INSERT','UPDATE')
		and (
		nvl(LPN_ID, 0) ,
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(LPN_ID, 0) as LPN_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			LPN_ID 
			)	
	);

commit;

-- Soft delete
update bec_ods.WMS_LICENSE_PLATE_NUMBERS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WMS_LICENSE_PLATE_NUMBERS set IS_DELETED_FLG = 'Y'
where (LPN_ID)  in
(
select LPN_ID from bec_raw_dl_ext.WMS_LICENSE_PLATE_NUMBERS
where (LPN_ID,KCA_SEQ_ID)
in 
(
select LPN_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WMS_LICENSE_PLATE_NUMBERS
group by LPN_ID
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
	ods_table_name = 'wms_license_plate_numbers';

commit;