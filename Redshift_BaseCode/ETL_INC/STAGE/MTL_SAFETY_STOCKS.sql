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
	table bec_ods_stg.MTL_SAFETY_STOCKS;

COMMIT;

insert
	into
	bec_ods_stg.MTL_SAFETY_STOCKS
(	
	INVENTORY_ITEM_ID,
	ORGANIZATION_ID,
	EFFECTIVITY_DATE, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY,
	CREATION_DATE, 
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	SAFETY_STOCK_CODE,
	SAFETY_STOCK_QUANTITY,
	SAFETY_STOCK_PERCENT,
	SERVICE_LEVEL,
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
	REQUEST_ID,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	PROGRAM_UPDATE_DATE, 
	FORECAST_DESIGNATOR, 
	PROJECT_ID,
	TASK_ID,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		INVENTORY_ITEM_ID,
		ORGANIZATION_ID,
		EFFECTIVITY_DATE, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY,
		CREATION_DATE, 
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		SAFETY_STOCK_CODE,
		SAFETY_STOCK_QUANTITY,
		SAFETY_STOCK_PERCENT,
		SERVICE_LEVEL,
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
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE, 
		FORECAST_DESIGNATOR, 
		PROJECT_ID,
		TASK_ID,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.MTL_SAFETY_STOCKS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(INVENTORY_ITEM_ID, 0),
		nvl(ORGANIZATION_ID, 0),
		nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00'),
		kca_seq_id) in 
(
		select
			nvl(INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID,
			nvl(ORGANIZATION_ID,0) as ORGANIZATION_ID,
			nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00') as EFFECTIVITY_DATE,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.MTL_SAFETY_STOCKS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(INVENTORY_ITEM_ID, 0),
			nvl(ORGANIZATION_ID, 0),
			nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00'))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_safety_stocks')
);
end;