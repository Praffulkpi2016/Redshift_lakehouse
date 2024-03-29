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

truncate table bec_ods_stg.wsh_carriers;

insert into	bec_ods_stg.wsh_carriers
   (	
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
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
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
		kca_seq_id
		,KCA_SEQ_DATE
	from bec_raw_dl_ext.wsh_carriers
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (nvl(CARRIER_ID,0),kca_seq_id) in 
	(select nvl(CARRIER_ID,0) as CARRIER_ID,max(kca_seq_id) from bec_raw_dl_ext.wsh_carriers 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by nvl(CARRIER_ID,0))
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'wsh_carriers')
)
);
end;