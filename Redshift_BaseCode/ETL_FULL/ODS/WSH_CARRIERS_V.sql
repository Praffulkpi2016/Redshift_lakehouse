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

DROP TABLE if exists bec_ods.WSH_CARRIERS_V;

CREATE TABLE IF NOT EXISTS bec_ods.WSH_CARRIERS_V
(
	carrier_id NUMERIC(15,0)   ENCODE az64
	,scac_code VARCHAR(4)   ENCODE lzo
	,freight_code VARCHAR(30)   ENCODE lzo
	,manifesting_enabled_flag VARCHAR(1)   ENCODE lzo
	,currency_code VARCHAR(15)   ENCODE lzo
	,carrier_name VARCHAR(360)   ENCODE lzo
	,active VARCHAR(1)   ENCODE lzo
	,party_usg_assignment_id NUMERIC(15,0)   ENCODE az64
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
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(38,0)   ENCODE az64
	,max_num_stops_permitted NUMERIC(28,10)   ENCODE az64
	,max_total_distance NUMERIC(28,10)    ENCODE az64
	,max_total_time NUMERIC(28,10)    ENCODE az64
	,allow_intersperse_load VARCHAR(1)   ENCODE lzo
	,max_layover_time NUMERIC(28,10)    ENCODE az64
	,min_layover_time NUMERIC(28,10)    ENCODE az64
	,max_total_distance_in_24hr NUMERIC(28,10)    ENCODE az64
	,max_driving_time_in_24hr NUMERIC(28,10)    ENCODE az64
	,max_duty_time_in_24hr NUMERIC(28,10)    ENCODE az64
	,allow_continuous_move VARCHAR(1)   ENCODE lzo
	,max_cm_distance NUMERIC(28,10)    ENCODE az64
	,max_cm_time NUMERIC(28,10)    ENCODE az64
	,max_cm_dh_distance NUMERIC(28,10)    ENCODE az64
	,max_cm_dh_time NUMERIC(28,10)    ENCODE az64
	,max_size_width NUMERIC(28,10)    ENCODE az64
	,max_size_height NUMERIC(28,10)    ENCODE az64
	,max_size_length NUMERIC(28,10)    ENCODE az64
	,min_size_width NUMERIC(28,10)    ENCODE az64
	,min_size_height NUMERIC(28,10)    ENCODE az64
	,min_size_length NUMERIC(28,10)    ENCODE az64
	,time_uom VARCHAR(10)   ENCODE lzo
	,dimension_uom VARCHAR(3)   ENCODE lzo
	,distance_uom VARCHAR(3)   ENCODE lzo
	,max_out_of_route NUMERIC(28,10)    ENCODE az64
	,cm_free_dh_mileage NUMERIC(28,10)   ENCODE az64
	,min_cm_distance NUMERIC(28,10)    ENCODE az64
	,cm_first_load_discount VARCHAR(1)   ENCODE lzo
	,min_cm_time NUMERIC(28,10)    ENCODE az64
	,unit_rate_basis VARCHAR(30)   ENCODE lzo
	,weight_uom VARCHAR(3)   ENCODE lzo
	,volume_uom VARCHAR(3)   ENCODE lzo
	,generic_flag VARCHAR(1)   ENCODE lzo
	,freight_bill_auto_approval VARCHAR(1)   ENCODE lzo
	,freight_audit_line_level VARCHAR(1)   ENCODE lzo
	,supplier_id NUMERIC(15,0)   ENCODE az64
	,supplier_site_id NUMERIC(15,0)   ENCODE az64
	,cm_rate_variant VARCHAR(30)   ENCODE lzo
	,distance_calculation_method VARCHAR(30)   ENCODE lzo
	,origin_dstn_surcharge_level VARCHAR(30)   ENCODE lzo
	,dim_dimensional_factor NUMERIC(28,10)   ENCODE az64
	,dim_weight_uom VARCHAR(3)   ENCODE lzo
	,dim_volume_uom VARCHAR(3)   ENCODE lzo
	,dim_dimension_uom VARCHAR(3)   ENCODE lzo
	,dim_min_pack_vol NUMERIC(28,10)    ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.WSH_CARRIERS_V (
   	carrier_id,
	scac_code,
	freight_code,
	manifesting_enabled_flag,
	currency_code,
	carrier_name,
	active,
	party_usg_assignment_id,
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
	max_num_stops_permitted,
	max_total_distance,
	max_total_time,
	allow_intersperse_load,
	max_layover_time,
	min_layover_time,
	max_total_distance_in_24hr,
	max_driving_time_in_24hr,
	max_duty_time_in_24hr,
	allow_continuous_move,
	max_cm_distance,
	max_cm_time,
	max_cm_dh_distance,
	max_cm_dh_time,
	max_size_width,
	max_size_height,
	max_size_length,
	min_size_width,
	min_size_height,
	min_size_length,
	time_uom,
	dimension_uom,
	distance_uom,
	max_out_of_route,
	cm_free_dh_mileage,
	min_cm_distance,
	cm_first_load_discount,
	min_cm_time,
	unit_rate_basis,
	weight_uom,
	volume_uom,
	generic_flag,
	freight_bill_auto_approval,
	freight_audit_line_level,
	supplier_id,
	supplier_site_id,
	cm_rate_variant,
	distance_calculation_method,
	origin_dstn_surcharge_level,
	dim_dimensional_factor,
	dim_weight_uom,
	dim_volume_uom,
	dim_dimension_uom,
	dim_min_pack_vol,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
       	carrier_id,
	scac_code,
	freight_code,
	manifesting_enabled_flag,
	currency_code,
	carrier_name,
	active,
	party_usg_assignment_id,
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
	max_num_stops_permitted,
	max_total_distance,
	max_total_time,
	allow_intersperse_load,
	max_layover_time,
	min_layover_time,
	max_total_distance_in_24hr,
	max_driving_time_in_24hr,
	max_duty_time_in_24hr,
	allow_continuous_move,
	max_cm_distance,
	max_cm_time,
	max_cm_dh_distance,
	max_cm_dh_time,
	max_size_width,
	max_size_height,
	max_size_length,
	min_size_width,
	min_size_height,
	min_size_length,
	time_uom,
	dimension_uom,
	distance_uom,
	max_out_of_route,
	cm_free_dh_mileage,
	min_cm_distance,
	cm_first_load_discount,
	min_cm_time,
	unit_rate_basis,
	weight_uom,
	volume_uom,
	generic_flag,
	freight_bill_auto_approval,
	freight_audit_line_level,
	supplier_id,
	supplier_site_id,
	cm_rate_variant,
	distance_calculation_method,
	origin_dstn_surcharge_level,
	dim_dimensional_factor,
	dim_weight_uom,
	dim_volume_uom,
	dim_dimension_uom,
	dim_min_pack_vol,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.WSH_CARRIERS_V;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wsh_carriers_v';
	
COMMIT;