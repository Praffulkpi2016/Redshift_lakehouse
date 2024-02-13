/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.MTL_INTERORG_SHIP_METHODS;

CREATE TABLE  IF NOT EXISTS bec_ods.MTL_INTERORG_SHIP_METHODS
(
	FROM_ORGANIZATION_ID NUMERIC(15,0)   ENCODE az64, 
TO_ORGANIZATION_ID NUMERIC(15,0)   ENCODE az64, 
SHIP_METHOD VARCHAR(30) ENCODE lzo, 
LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64, 
LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64 NOT NULL , 
CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64, 
CREATED_BY NUMERIC(15,0)   ENCODE az64 NOT NULL , 
LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64, 
TIME_UOM_CODE VARCHAR(3) ENCODE lzo, 
INTRANSIT_TIME NUMERIC(15,0)   ENCODE az64 NOT NULL , 
DEFAULT_FLAG NUMERIC(15,0)   ENCODE az64, 
FROM_LOCATION_ID NUMERIC(15,0)   ENCODE az64, 
TO_LOCATION_ID NUMERIC(15,0)   ENCODE az64, 
LOAD_WEIGHT_UOM_CODE VARCHAR(3) ENCODE lzo, 
VOLUME_UOM_CODE VARCHAR(3) ENCODE lzo, 
CURRENCY_CODE VARCHAR(15) ENCODE lzo, 
DAILY_LOAD_WEIGHT_CAPACITY NUMERIC(28,10)   ENCODE az64, 
COST_PER_UNIT_LOAD_WEIGHT NUMERIC(28,10)   ENCODE az64, 
DAILY_VOLUME_CAPACITY NUMERIC(28,10)   ENCODE az64, 
COST_PER_UNIT_LOAD_VOLUME NUMERIC(28,10)   ENCODE az64, 
ATTRIBUTE_CATEGORY VARCHAR(30) ENCODE lzo, 
ATTRIBUTE1 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE2 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE3 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE4 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE5 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE6 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE7 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE8 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE9 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE10 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE11 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE12 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE13 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE14 VARCHAR(150)   ENCODE lzo, 
ATTRIBUTE15 VARCHAR(150)   ENCODE lzo, 
TO_REGION_ID NUMERIC(15,0)   ENCODE az64, 
DESTINATION_TYPE VARCHAR(1) ENCODE lzo, 
ORIGIN_TYPE VARCHAR(1) ENCODE lzo, 
FROM_REGION_ID NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
 	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
	
insert
	into
	bec_ods.MTL_INTERORG_SHIP_METHODS
(from_organization_id,
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
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
	SELECT
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
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MTL_INTERORG_SHIP_METHODS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_interorg_ship_methods';
	
commit;
	
