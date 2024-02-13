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

DROP TABLE if exists bec_ods.WSH_CARRIER_SERVICES_V;

CREATE TABLE IF NOT EXISTS bec_ods.WSH_CARRIER_SERVICES_V
(
	carrier_service_id NUMERIC(15,0)   ENCODE az64
	,carrier_id NUMERIC(38,0)   ENCODE az64
	,service_level VARCHAR(30)   ENCODE lzo
	,mode_of_transport VARCHAR(30)   ENCODE lzo
	,sl_time_uom VARCHAR(10)   ENCODE lzo
	,min_sl_time NUMERIC(38,0)   ENCODE az64
	,max_sl_time NUMERIC(38,0)   ENCODE az64
	,enabled_flag VARCHAR(1)   ENCODE lzo
	,web_enabled VARCHAR(1)   ENCODE lzo
	,ship_method_code VARCHAR(30)   ENCODE lzo
	,ship_method_meaning VARCHAR(80)   ENCODE lzo
	,attribute_category VARCHAR(150)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(38,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(38,0)   ENCODE az64
	,last_update_login NUMERIC(38,0)   ENCODE az64
	,program_application_id NUMERIC(38,0)   ENCODE az64
	,program_id NUMERIC(38,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(38,0)   ENCODE az64
	,max_num_stops_permitted NUMERIC(28,10)   ENCODE az64
	,max_total_distance NUMERIC(28,10)   ENCODE az64
	,max_total_time NUMERIC(28,10)   ENCODE az64
	,allow_intersperse_load VARCHAR(1)   ENCODE lzo
	,max_layover_time NUMERIC(28,10)   ENCODE az64
	,min_layover_time NUMERIC(28,10)   ENCODE az64
	,max_total_distance_in_24hr NUMERIC(28,10)    ENCODE az64
	,allow_continuous_move VARCHAR(1)   ENCODE lzo
	,max_cm_distance NUMERIC(28,10)    ENCODE az64
	,max_cm_time NUMERIC(28,10)    ENCODE az64
	,max_cm_dh_distance NUMERIC(28,10)    ENCODE az64
	,max_cm_dh_time NUMERIC(28,10)    ENCODE az64
	,max_size_length NUMERIC(28,10)    ENCODE az64
	,max_size_width NUMERIC(28,10)    ENCODE az64
	,max_size_height NUMERIC(28,10)    ENCODE az64
	,min_size_length NUMERIC(28,10)    ENCODE az64
	,min_size_width NUMERIC(28,10)    ENCODE az64
	,min_size_height NUMERIC(28,10)    ENCODE az64
	,max_out_of_route NUMERIC(28,10)    ENCODE az64
	,cm_free_dh_mileage NUMERIC(28,10)    ENCODE az64
	,min_cm_distance NUMERIC(28,10)    ENCODE az64
	,cm_first_load_discount VARCHAR(1)   ENCODE lzo
	,min_cm_time NUMERIC(28,10)    ENCODE az64
	,unit_rate_basis VARCHAR(30)   ENCODE lzo
	,cm_rate_variant VARCHAR(30)   ENCODE lzo
	,distance_calculation_method VARCHAR(30)   ENCODE lzo
	,origin_dstn_surcharge_level VARCHAR(30)   ENCODE lzo
	,max_driving_time_in_24hr NUMERIC(28,10)    ENCODE az64
	,max_duty_time_in_24hr NUMERIC(28,10)    ENCODE az64
	,dim_dimensional_factor NUMERIC(28,10)   ENCODE az64
	,dim_weight_uom VARCHAR(3)   ENCODE lzo
	,dim_volume_uom VARCHAR(3)   ENCODE lzo
	,dim_dimension_uom VARCHAR(3)   ENCODE lzo
	,dim_min_pack_vol NUMERIC(28,10)   ENCODE az64
	,default_vehicle_type_id NUMERIC(15,0)   ENCODE az64
	,update_mot_sl VARCHAR(1)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.WSH_CARRIER_SERVICES_V (
 carrier_service_id,
	carrier_id,
	service_level,
	mode_of_transport,
	sl_time_uom,
	min_sl_time,
	max_sl_time,
	enabled_flag,
	web_enabled,
	ship_method_code,
	ship_method_meaning,
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
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	max_num_stops_permitted,
	max_total_distance,
	max_total_time,
	allow_intersperse_load,
	max_layover_time,
	min_layover_time,
	max_total_distance_in_24hr,
	allow_continuous_move,
	max_cm_distance,
	max_cm_time,
	max_cm_dh_distance,
	max_cm_dh_time,
	max_size_length,
	max_size_width,
	max_size_height,
	min_size_length,
	min_size_width,
	min_size_height,
	max_out_of_route,
	cm_free_dh_mileage,
	min_cm_distance,
	cm_first_load_discount,
	min_cm_time,
	unit_rate_basis,
	cm_rate_variant,
	distance_calculation_method,
	origin_dstn_surcharge_level,
	max_driving_time_in_24hr,
	max_duty_time_in_24hr,
	dim_dimensional_factor,
	dim_weight_uom,
	dim_volume_uom,
	dim_dimension_uom,
	dim_min_pack_vol,
	default_vehicle_type_id,
	update_mot_sl,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
       carrier_service_id,
	carrier_id,
	service_level,
	mode_of_transport,
	sl_time_uom,
	min_sl_time,
	max_sl_time,
	enabled_flag,
	web_enabled,
	ship_method_code,
	ship_method_meaning,
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
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	max_num_stops_permitted,
	max_total_distance,
	max_total_time,
	allow_intersperse_load,
	max_layover_time,
	min_layover_time,
	max_total_distance_in_24hr,
	allow_continuous_move,
	max_cm_distance,
	max_cm_time,
	max_cm_dh_distance,
	max_cm_dh_time,
	max_size_length,
	max_size_width,
	max_size_height,
	min_size_length,
	min_size_width,
	min_size_height,
	max_out_of_route,
	cm_free_dh_mileage,
	min_cm_distance,
	cm_first_load_discount,
	min_cm_time,
	unit_rate_basis,
	cm_rate_variant,
	distance_calculation_method,
	origin_dstn_surcharge_level,
	max_driving_time_in_24hr,
	max_duty_time_in_24hr,
	dim_dimensional_factor,
	dim_weight_uom,
	dim_volume_uom,
	dim_dimension_uom,
	dim_min_pack_vol,
	default_vehicle_type_id,
	update_mot_sl,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.WSH_CARRIER_SERVICES_V;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wsh_carrier_services_v';
	
COMMIT;