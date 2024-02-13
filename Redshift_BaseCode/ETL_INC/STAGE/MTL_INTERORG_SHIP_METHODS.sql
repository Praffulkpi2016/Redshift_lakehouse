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

truncate table bec_ods_stg.MTL_INTERORG_SHIP_METHODS;

insert into	bec_ods_stg.MTL_INTERORG_SHIP_METHODS
   (
	from_organization_id,
	to_organization_id,
	ship_method,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	time_uom_code,
	intransit_time,
	default_flag,
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
	from_location_id,
	to_location_id,
	load_weight_uom_code,
	volume_uom_code,
	currency_code,
	daily_load_weight_capacity,
	cost_per_unit_load_weight,
	daily_volume_capacity,
	cost_per_unit_load_volume,
	to_region_id,
	destination_type,
	origin_type,
	from_region_id,
	kca_operation,
	kca_seq_id,
	kca_seq_date)
(
	select
		
	from_organization_id,
	to_organization_id,
	ship_method,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	time_uom_code,
	intransit_time,
	default_flag,
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
	from_location_id,
	to_location_id,
	load_weight_uom_code,
	volume_uom_code,
	currency_code,
	daily_load_weight_capacity,
	cost_per_unit_load_weight,
	daily_volume_capacity,
	cost_per_unit_load_volume,
	to_region_id,
	destination_type,
	origin_type,
	from_region_id,
	kca_operation,
	kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.MTL_INTERORG_SHIP_METHODS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (nvl(FROM_ORGANIZATION_ID,0),nvl(TO_ORGANIZATION_ID,0),
nvl(FROM_LOCATION_ID,0),nvl(TO_LOCATION_ID,0),nvl(SHIP_METHOD,'0'),kca_seq_id) in 
	(select nvl(FROM_ORGANIZATION_ID,0),nvl(TO_ORGANIZATION_ID,0),nvl(FROM_LOCATION_ID,0),nvl(TO_LOCATION_ID,0),
nvl(SHIP_METHOD,'0'),max(kca_seq_id) from bec_raw_dl_ext.MTL_INTERORG_SHIP_METHODS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by nvl(FROM_ORGANIZATION_ID,0),nvl(TO_ORGANIZATION_ID,0),nvl(FROM_LOCATION_ID,0),nvl(TO_LOCATION_ID,0),
nvl(SHIP_METHOD,'0')
	 )
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_interorg_ship_methods')
		 
            )
);
end;
