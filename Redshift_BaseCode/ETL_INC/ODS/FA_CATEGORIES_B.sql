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

delete from bec_ods.FA_CATEGORIES_B
where (nvl(CATEGORY_ID,0)) in (
select nvl(stg.CATEGORY_ID,0) as CATEGORY_ID from bec_ods.FA_CATEGORIES_B ods, bec_ods_stg.FA_CATEGORIES_B stg
where nvl(ods.CATEGORY_ID,0) = nvl(stg.CATEGORY_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.FA_CATEGORIES_B
       (	
		CATEGORY_ID, 
		SUMMARY_FLAG, 
		ENABLED_FLAG, 
		OWNED_LEASED, 
		PRODUCTION_CAPACITY, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CATEGORY_TYPE, 
		CAPITALIZE_FLAG, 
		SEGMENT1, 
		SEGMENT2, 
		SEGMENT3, 
		SEGMENT4, 
		SEGMENT5, 
		SEGMENT6, 
		SEGMENT7, 
		START_DATE_ACTIVE, 
		END_DATE_ACTIVE, 
		PROPERTY_TYPE_CODE, 
		PROPERTY_1245_1250_CODE, 
		DATE_INEFFECTIVE, 
		INVENTORIAL, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
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
		ATTRIBUTE_CATEGORY_CODE, 
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
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		CATEGORY_ID, 
		SUMMARY_FLAG, 
		ENABLED_FLAG, 
		OWNED_LEASED, 
		PRODUCTION_CAPACITY, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CATEGORY_TYPE, 
		CAPITALIZE_FLAG, 
		SEGMENT1, 
		SEGMENT2, 
		SEGMENT3, 
		SEGMENT4, 
		SEGMENT5, 
		SEGMENT6, 
		SEGMENT7, 
		START_DATE_ACTIVE, 
		END_DATE_ACTIVE, 
		PROPERTY_TYPE_CODE, 
		PROPERTY_1245_1250_CODE, 
		DATE_INEFFECTIVE, 
		INVENTORIAL, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
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
		ATTRIBUTE_CATEGORY_CODE, 
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.FA_CATEGORIES_B
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(CATEGORY_ID,0),kca_seq_id) in 
	(select nvl(CATEGORY_ID,0) as CATEGORY_ID,max(kca_seq_id) from bec_ods_stg.FA_CATEGORIES_B 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(CATEGORY_ID,0))
);

commit;

-- Soft delete
update bec_ods.FA_CATEGORIES_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_CATEGORIES_B set IS_DELETED_FLG = 'Y'
where (CATEGORY_ID)  in
(
select CATEGORY_ID from bec_raw_dl_ext.FA_CATEGORIES_B
where (CATEGORY_ID,KCA_SEQ_ID)
in 
(
select CATEGORY_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_CATEGORIES_B
group by CATEGORY_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fa_categories_b';

commit;