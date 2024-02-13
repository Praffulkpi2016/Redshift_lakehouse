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

DROP TABLE if exists bec_ods.wsh_carriers;

CREATE TABLE IF NOT EXISTS bec_ods.wsh_carriers
(
	CARRIER_ID NUMERIC(15,0) ENCODE az64 
	,FREIGHT_CODE VARCHAR(30) ENCODE lzo
	,SCAC_CODE VARCHAR(4) ENCODE lzo
	,MANIFESTING_ENABLED_FLAG VARCHAR(1) ENCODE lzo
	,CURRENCY_CODE VARCHAR(15) ENCODE lzo
	,ATTRIBUTE_CATEGORY VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE1 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE2 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE3 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE4 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE5 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE6 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE7 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE8 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE9 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE10 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE11 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE12 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE13 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE14 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE15 VARCHAR(150) ENCODE lzo	
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,CREATED_BY NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64
    ,GENERIC_FLAG VARCHAR(1) ENCODE lzo
    ,FREIGHT_BILL_AUTO_APPROVAL VARCHAR(1) ENCODE lzo
    ,FREIGHT_AUDIT_LINE_LEVEL VARCHAR(1) ENCODE lzo
	,SUPPLIER_ID NUMERIC(15,0) ENCODE az64 
	,SUPPLIER_SITE_ID NUMERIC(15,0) ENCODE az64 
	,MAX_NUM_STOPS_PERMITTED NUMERIC(28,10) ENCODE az64  
	,MAX_TOTAL_DISTANCE NUMERIC(28,10) ENCODE az64 
	,MAX_TOTAL_TIME NUMERIC(28,10) ENCODE az64
    ,ALLOW_INTERSPERSE_LOAD VARCHAR(1) ENCODE lzo
	,MIN_LAYOVER_TIME NUMERIC(28,10) ENCODE az64  
	,MAX_LAYOVER_TIME NUMERIC(28,10) ENCODE az64 
	,MAX_TOTAL_DISTANCE_IN_24HR NUMERIC(28,10) ENCODE az64  
	,MAX_DRIVING_TIME_IN_24HR NUMERIC(28,10) ENCODE az64 
	,MAX_DUTY_TIME_IN_24HR NUMERIC(28,10) ENCODE az64 
    ,ALLOW_CONTINUOUS_MOVE VARCHAR(1) ENCODE lzo
	,MAX_CM_DISTANCE NUMERIC(28,10) ENCODE az64  
	,MAX_CM_TIME NUMERIC(28,10) ENCODE az64 
	,MAX_CM_DH_DISTANCE NUMERIC(28,10) ENCODE az64 
	,MAX_CM_DH_TIME NUMERIC(28,10) ENCODE az64  
	,MIN_CM_DISTANCE NUMERIC(28,10) ENCODE az64 
	,MIN_CM_TIME NUMERIC(28,10) ENCODE az64 	
	,CM_FREE_DH_MILEAGE NUMERIC(28,10) ENCODE az64 
    ,CM_FIRST_LOAD_DISCOUNT VARCHAR(1) ENCODE lzo
    ,CM_RATE_VARIANT VARCHAR(30) ENCODE lzo 
	,MAX_SIZE_WIDTH NUMERIC(28,10) ENCODE az64 
	,MAX_SIZE_HEIGHT NUMERIC(28,10) ENCODE az64 
	,MAX_SIZE_LENGTH NUMERIC(28,10) ENCODE az64  
	,MIN_SIZE_WIDTH NUMERIC(28,10) ENCODE az64 
	,MIN_SIZE_HEIGHT NUMERIC(28,10) ENCODE az64 	
	,MIN_SIZE_LENGTH NUMERIC(28,10) ENCODE az64 
	,TIME_UOM VARCHAR(10)   ENCODE lzo
    ,DIMENSION_UOM VARCHAR(3) ENCODE lzo
    ,DISTANCE_UOM VARCHAR(3) ENCODE lzo
    ,WEIGHT_UOM VARCHAR(3) ENCODE lzo
    ,VOLUME_UOM VARCHAR(3) ENCODE lzo	
	,MAX_OUT_OF_ROUTE NUMERIC(28,10) ENCODE az64 
    ,DISTANCE_CALCULATION_METHOD VARCHAR(30) ENCODE lzo 
    ,ORIGIN_DSTN_SURCHARGE_LEVEL VARCHAR(30) ENCODE lzo 
    ,UNIT_RATE_BASIS VARCHAR(30) ENCODE lzo 	
	,DIM_DIMENSIONAL_FACTOR NUMERIC(28,10) ENCODE az64 
    ,DIM_WEIGHT_UOM VARCHAR(3) ENCODE lzo
    ,DIM_VOLUME_UOM VARCHAR(3) ENCODE lzo
    ,DIM_DIMENSION_UOM VARCHAR(3) ENCODE lzo	
	,DIM_MIN_PACK_VOL NUMERIC(28,10) ENCODE az64 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.wsh_carriers (
	CARRIER_ID,
	FREIGHT_CODE,
	SCAC_CODE,
	MANIFESTING_ENABLED_FLAG,
	CURRENCY_CODE,
	ATTRIBUTE_CATEGORY,
	ATTRIBUTE1,
	ATTRIBUTE2,
	ATTRIBUTE3,
	ATTRIBUTE4,
	ATTRIBUTE5,
	ATTRIBUTE6,
	ATTRIBUTE7,
	ATTRIBUTE8,
	ATTRIBUTE9,
	ATTRIBUTE10,
	ATTRIBUTE11,
	ATTRIBUTE12,
	ATTRIBUTE13,
	ATTRIBUTE14,
	ATTRIBUTE15,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	GENERIC_FLAG,
	FREIGHT_BILL_AUTO_APPROVAL,
	FREIGHT_AUDIT_LINE_LEVEL,
	SUPPLIER_ID,
	SUPPLIER_SITE_ID,
	MAX_NUM_STOPS_PERMITTED,
	MAX_TOTAL_DISTANCE,
	MAX_TOTAL_TIME,
	ALLOW_INTERSPERSE_LOAD,
	MIN_LAYOVER_TIME,
	MAX_LAYOVER_TIME,
	MAX_TOTAL_DISTANCE_IN_24HR,
	MAX_DRIVING_TIME_IN_24HR,
	MAX_DUTY_TIME_IN_24HR,
	ALLOW_CONTINUOUS_MOVE,
	MAX_CM_DISTANCE,
	MAX_CM_TIME,
	MAX_CM_DH_DISTANCE,
	MAX_CM_DH_TIME,
	MIN_CM_DISTANCE,
	MIN_CM_TIME,
	CM_FREE_DH_MILEAGE,
	CM_FIRST_LOAD_DISCOUNT,
	CM_RATE_VARIANT,
	MAX_SIZE_WIDTH,
	MAX_SIZE_HEIGHT,
	MAX_SIZE_LENGTH,
	MIN_SIZE_WIDTH,
	MIN_SIZE_HEIGHT,
	MIN_SIZE_LENGTH,
	TIME_UOM,
	DIMENSION_UOM,
	DISTANCE_UOM,
	WEIGHT_UOM,
	VOLUME_UOM,
	MAX_OUT_OF_ROUTE,
	DISTANCE_CALCULATION_METHOD,
	ORIGIN_DSTN_SURCHARGE_LEVEL,
	UNIT_RATE_BASIS,
	DIM_DIMENSIONAL_FACTOR,
	DIM_WEIGHT_UOM,
	DIM_VOLUME_UOM,
	DIM_DIMENSION_UOM,
	DIM_MIN_PACK_VOL,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
		CARRIER_ID,
		FREIGHT_CODE,
		SCAC_CODE,
		MANIFESTING_ENABLED_FLAG,
		CURRENCY_CODE,
		ATTRIBUTE_CATEGORY,
		ATTRIBUTE1,
		ATTRIBUTE2,
		ATTRIBUTE3,
		ATTRIBUTE4,
		ATTRIBUTE5,
		ATTRIBUTE6,
		ATTRIBUTE7,
		ATTRIBUTE8,
		ATTRIBUTE9,
		ATTRIBUTE10,
		ATTRIBUTE11,
		ATTRIBUTE12,
		ATTRIBUTE13,
		ATTRIBUTE14,
		ATTRIBUTE15,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		GENERIC_FLAG,
		FREIGHT_BILL_AUTO_APPROVAL,
		FREIGHT_AUDIT_LINE_LEVEL,
		SUPPLIER_ID,
		SUPPLIER_SITE_ID,
		MAX_NUM_STOPS_PERMITTED,
		MAX_TOTAL_DISTANCE,
		MAX_TOTAL_TIME,
		ALLOW_INTERSPERSE_LOAD,
		MIN_LAYOVER_TIME,
		MAX_LAYOVER_TIME,
		MAX_TOTAL_DISTANCE_IN_24HR,
		MAX_DRIVING_TIME_IN_24HR,
		MAX_DUTY_TIME_IN_24HR,
		ALLOW_CONTINUOUS_MOVE,
		MAX_CM_DISTANCE,
		MAX_CM_TIME,
		MAX_CM_DH_DISTANCE,
		MAX_CM_DH_TIME,
		MIN_CM_DISTANCE,
		MIN_CM_TIME,
		CM_FREE_DH_MILEAGE,
		CM_FIRST_LOAD_DISCOUNT,
		CM_RATE_VARIANT,
		MAX_SIZE_WIDTH,
		MAX_SIZE_HEIGHT,
		MAX_SIZE_LENGTH,
		MIN_SIZE_WIDTH,
		MIN_SIZE_HEIGHT,
		MIN_SIZE_LENGTH,
		TIME_UOM,
		DIMENSION_UOM,
		DISTANCE_UOM,
		WEIGHT_UOM,
		VOLUME_UOM,
		MAX_OUT_OF_ROUTE,
		DISTANCE_CALCULATION_METHOD,
		ORIGIN_DSTN_SURCHARGE_LEVEL,
		UNIT_RATE_BASIS,
		DIM_DIMENSIONAL_FACTOR,
		DIM_WEIGHT_UOM,
		DIM_VOLUME_UOM,
		DIM_DIMENSION_UOM,
		DIM_MIN_PACK_VOL,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.wsh_carriers;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wsh_carriers';
	
commit;