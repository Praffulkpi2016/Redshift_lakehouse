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

delete from bec_ods.MTL_INTERORG_SHIP_METHODS
where (FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,FROM_LOCATION_ID,TO_LOCATION_ID,SHIP_METHOD) in (
select stg.FROM_ORGANIZATION_ID,stg.TO_ORGANIZATION_ID,stg.FROM_LOCATION_ID,stg.TO_LOCATION_ID,stg.SHIP_METHOD
from bec_ods.MTL_INTERORG_SHIP_METHODS ods, bec_ods_stg.MTL_INTERORG_SHIP_METHODS stg
where 
ods.FROM_ORGANIZATION_ID = stg.FROM_ORGANIZATION_ID and 
ods.TO_ORGANIZATION_ID = stg.TO_ORGANIZATION_ID and 
ods.FROM_LOCATION_ID = stg.FROM_LOCATION_ID  AND 
ods.TO_LOCATION_ID = stg.TO_LOCATION_ID AND 
ods.SHIP_METHOD = stg.SHIP_METHOD 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_INTERORG_SHIP_METHODS
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
	is_deleted_flg,
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.MTL_INTERORG_SHIP_METHODS
	where kca_operation IN ('INSERT','UPDATE') 
	and (FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,FROM_LOCATION_ID,TO_LOCATION_ID,SHIP_METHOD,kca_seq_id) in 
	(select FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,FROM_LOCATION_ID,TO_LOCATION_ID,SHIP_METHOD,max(kca_seq_id) from bec_ods_stg.MTL_INTERORG_SHIP_METHODS 
     where kca_operation IN ('INSERT','UPDATE')
     group by FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,FROM_LOCATION_ID,TO_LOCATION_ID,SHIP_METHOD)
);

commit;
 
-- Soft delete
update bec_ods.MTL_INTERORG_SHIP_METHODS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_INTERORG_SHIP_METHODS set IS_DELETED_FLG = 'Y'
where (FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,FROM_LOCATION_ID,TO_LOCATION_ID,SHIP_METHOD)  in
(
select FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,FROM_LOCATION_ID,TO_LOCATION_ID,SHIP_METHOD from bec_raw_dl_ext.MTL_INTERORG_SHIP_METHODS
where (FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,FROM_LOCATION_ID,TO_LOCATION_ID,SHIP_METHOD,KCA_SEQ_ID)
in 
(
select FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,FROM_LOCATION_ID,TO_LOCATION_ID,SHIP_METHOD,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_INTERORG_SHIP_METHODS
group by FROM_ORGANIZATION_ID,TO_ORGANIZATION_ID,FROM_LOCATION_ID,TO_LOCATION_ID,SHIP_METHOD
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_interorg_ship_methods';

commit;