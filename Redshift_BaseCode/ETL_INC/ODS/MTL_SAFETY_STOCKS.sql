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

delete from bec_ods.MTL_SAFETY_STOCKS
where (nvl(INVENTORY_ITEM_ID,0),nvl(ORGANIZATION_ID,0),nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00')) in (
select nvl(stg.INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID,
nvl(stg.ORGANIZATION_ID,0) as ORGANIZATION_ID,
nvl(stg.EFFECTIVITY_DATE, '1900-01-01 12:00:00') as EFFECTIVITY_DATE
from bec_ods.MTL_SAFETY_STOCKS ods, bec_ods_stg.MTL_SAFETY_STOCKS stg
where nvl(ods.INVENTORY_ITEM_ID,0) = nvl(stg.INVENTORY_ITEM_ID,0) 
and nvl(ods.ORGANIZATION_ID,0) = nvl(stg.ORGANIZATION_ID,0)
and nvl(ods.EFFECTIVITY_DATE, '1900-01-01 12:00:00') = nvl(stg.EFFECTIVITY_DATE, '1900-01-01 12:00:00')
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_SAFETY_STOCKS
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
        IS_DELETED_FLG,
		kca_seq_ID,
		kca_seq_date)	
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.MTL_SAFETY_STOCKS
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(INVENTORY_ITEM_ID,0),nvl(ORGANIZATION_ID,0),nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00'),kca_seq_ID) in 
	(select nvl(INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID,nvl(ORGANIZATION_ID,0) as ORGANIZATION_ID,nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00') as EFFECTIVITY_DATE,max(kca_seq_ID) as kca_seq_ID from bec_ods_stg.MTL_SAFETY_STOCKS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(INVENTORY_ITEM_ID,0),nvl(ORGANIZATION_ID,0),nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00'))
);

commit;

-- Soft delete
update bec_ods.MTL_SAFETY_STOCKS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_SAFETY_STOCKS set IS_DELETED_FLG = 'Y'
where (nvl(INVENTORY_ITEM_ID,0),nvl(ORGANIZATION_ID,0),nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00'))  in
(
select nvl(INVENTORY_ITEM_ID,0),nvl(ORGANIZATION_ID,0),nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00') from bec_raw_dl_ext.MTL_SAFETY_STOCKS
where (nvl(INVENTORY_ITEM_ID,0),nvl(ORGANIZATION_ID,0),nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00'),KCA_SEQ_ID)
in 
(
select nvl(INVENTORY_ITEM_ID,0),nvl(ORGANIZATION_ID,0),nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_SAFETY_STOCKS
group by nvl(INVENTORY_ITEM_ID,0),nvl(ORGANIZATION_ID,0),nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00')
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mtl_safety_stocks';

commit;