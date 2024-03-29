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

delete from bec_ods.FA_ADDITIONS_B
where (nvl(ASSET_ID,0)) in (
select nvl(stg.ASSET_ID,0) as ASSET_ID from bec_ods.FA_ADDITIONS_B ods, bec_ods_stg.FA_ADDITIONS_B stg
where nvl(ods.ASSET_ID,0) = nvl(stg.ASSET_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.FA_ADDITIONS_B
       (	
		ASSET_ID, 
		ASSET_NUMBER, 
		ASSET_KEY_CCID, 
		CURRENT_UNITS, 
		ASSET_TYPE, 
		TAG_NUMBER, 
		ASSET_CATEGORY_ID, 
		PARENT_ASSET_ID, 
		MANUFACTURER_NAME, 
		SERIAL_NUMBER, 
		MODEL_NUMBER, 
		PROPERTY_TYPE_CODE, 
		PROPERTY_1245_1250_CODE, 
		IN_USE_FLAG, 
		OWNED_LEASED, 
		NEW_USED, 
		UNIT_ADJUSTMENT_FLAG, 
		ADD_COST_JE_FLAG, 
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
		ATTRIBUTE16, 
		ATTRIBUTE17, 
		ATTRIBUTE18, 
		ATTRIBUTE19, 
		ATTRIBUTE20, 
		ATTRIBUTE21, 
		ATTRIBUTE22, 
		ATTRIBUTE23, 
		ATTRIBUTE24, 
		ATTRIBUTE25, 
		ATTRIBUTE26, 
		ATTRIBUTE27, 
		ATTRIBUTE28, 
		ATTRIBUTE29, 
		ATTRIBUTE30, 
		ATTRIBUTE_CATEGORY_CODE, 
		CONTEXT, 
		LEASE_ID, 
		INVENTORIAL, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
		GLOBAL_ATTRIBUTE1, 
		GLOBAL_ATTRIBUTE2, 
		GLOBAL_ATTRIBUTE3, 
		GLOBAL_ATTRIBUTE4, 
		GLOBAL_ATTRIBUTE5, 
		GLOBAL_ATTRIBUTE6, 
		GLOBAL_ATTRIBUTE7, 
		GLOBAL_ATTRIBUTE8, 
		GLOBAL_ATTRIBUTE9, 
		GLOBAL_ATTRIBUTE10, 
		GLOBAL_ATTRIBUTE11, 
		GLOBAL_ATTRIBUTE12, 
		GLOBAL_ATTRIBUTE13, 
		GLOBAL_ATTRIBUTE14, 
		GLOBAL_ATTRIBUTE15, 
		GLOBAL_ATTRIBUTE16, 
		GLOBAL_ATTRIBUTE17, 
		GLOBAL_ATTRIBUTE18, 
		GLOBAL_ATTRIBUTE19, 
		GLOBAL_ATTRIBUTE20, 
		GLOBAL_ATTRIBUTE_CATEGORY, 
		COMMITMENT, 
		INVESTMENT_LAW, 
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		ASSET_ID, 
		ASSET_NUMBER, 
		ASSET_KEY_CCID, 
		CURRENT_UNITS, 
		ASSET_TYPE, 
		TAG_NUMBER, 
		ASSET_CATEGORY_ID, 
		PARENT_ASSET_ID, 
		MANUFACTURER_NAME, 
		SERIAL_NUMBER, 
		MODEL_NUMBER, 
		PROPERTY_TYPE_CODE, 
		PROPERTY_1245_1250_CODE, 
		IN_USE_FLAG, 
		OWNED_LEASED, 
		NEW_USED, 
		UNIT_ADJUSTMENT_FLAG, 
		ADD_COST_JE_FLAG, 
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
		ATTRIBUTE16, 
		ATTRIBUTE17, 
		ATTRIBUTE18, 
		ATTRIBUTE19, 
		ATTRIBUTE20, 
		ATTRIBUTE21, 
		ATTRIBUTE22, 
		ATTRIBUTE23, 
		ATTRIBUTE24, 
		ATTRIBUTE25, 
		ATTRIBUTE26, 
		ATTRIBUTE27, 
		ATTRIBUTE28, 
		ATTRIBUTE29, 
		ATTRIBUTE30, 
		ATTRIBUTE_CATEGORY_CODE, 
		CONTEXT, 
		LEASE_ID, 
		INVENTORIAL, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
		GLOBAL_ATTRIBUTE1, 
		GLOBAL_ATTRIBUTE2, 
		GLOBAL_ATTRIBUTE3, 
		GLOBAL_ATTRIBUTE4, 
		GLOBAL_ATTRIBUTE5, 
		GLOBAL_ATTRIBUTE6, 
		GLOBAL_ATTRIBUTE7, 
		GLOBAL_ATTRIBUTE8, 
		GLOBAL_ATTRIBUTE9, 
		GLOBAL_ATTRIBUTE10, 
		GLOBAL_ATTRIBUTE11, 
		GLOBAL_ATTRIBUTE12, 
		GLOBAL_ATTRIBUTE13, 
		GLOBAL_ATTRIBUTE14, 
		GLOBAL_ATTRIBUTE15, 
		GLOBAL_ATTRIBUTE16, 
		GLOBAL_ATTRIBUTE17, 
		GLOBAL_ATTRIBUTE18, 
		GLOBAL_ATTRIBUTE19, 
		GLOBAL_ATTRIBUTE20, 
		GLOBAL_ATTRIBUTE_CATEGORY, 
		COMMITMENT, 
		INVESTMENT_LAW, 
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.FA_ADDITIONS_B
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(ASSET_ID,0),kca_seq_id) in 
	(select nvl(ASSET_ID,0) as ASSET_ID,max(kca_seq_id) from bec_ods_stg.FA_ADDITIONS_B 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(ASSET_ID,0))
);

commit;

-- Soft delete
update bec_ods.FA_ADDITIONS_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_ADDITIONS_B set IS_DELETED_FLG = 'Y'
where (ASSET_ID)  in
(
select ASSET_ID from bec_raw_dl_ext.FA_ADDITIONS_B
where (ASSET_ID,KCA_SEQ_ID)
in 
(
select ASSET_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_ADDITIONS_B
group by ASSET_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fa_additions_b';

commit;