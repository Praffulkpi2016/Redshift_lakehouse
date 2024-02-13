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

delete from bec_ods.hr_all_organization_units
where ORGANIZATION_ID in (
select stg.ORGANIZATION_ID from bec_ods.hr_all_organization_units ods, bec_ods_stg.hr_all_organization_units stg
where ods.ORGANIZATION_ID = stg.ORGANIZATION_ID  and stg.kca_operation IN ('INSERT','UPDATE') );

commit;

 -- Insert records

insert into bec_ods.hr_all_organization_units
(
ORGANIZATION_ID,
BUSINESS_GROUP_ID,
COST_ALLOCATION_KEYFLEX_ID,
LOCATION_ID,
SOFT_CODING_KEYFLEX_ID,
DATE_FROM,
NAME,
DATE_TO,
INTERNAL_EXTERNAL_FLAG,
INTERNAL_ADDRESS_LINE,
TYPE,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
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
ATTRIBUTE16,
ATTRIBUTE17,
ATTRIBUTE18,
ATTRIBUTE19,
ATTRIBUTE20,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
CREATED_BY,
CREATION_DATE,
OBJECT_VERSION_NUMBER,
PARTY_ID,
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
COMMENTS
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID ,
	kca_seq_date
)
(
select ORGANIZATION_ID,
BUSINESS_GROUP_ID,
COST_ALLOCATION_KEYFLEX_ID,
LOCATION_ID,
SOFT_CODING_KEYFLEX_ID,
DATE_FROM,
NAME,
DATE_TO,
INTERNAL_EXTERNAL_FLAG,
INTERNAL_ADDRESS_LINE,
TYPE,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
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
ATTRIBUTE16,
ATTRIBUTE17,
ATTRIBUTE18,
ATTRIBUTE19,
ATTRIBUTE20,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
CREATED_BY,
CREATION_DATE,
OBJECT_VERSION_NUMBER,
PARTY_ID,
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
COMMENTS
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID ,
	kca_seq_date
from bec_ods_stg.hr_all_organization_units
where kca_operation IN ('INSERT','UPDATE') and (ORGANIZATION_ID,kca_seq_id) in (select ORGANIZATION_ID,max(kca_seq_id) from bec_ods_stg.hr_all_organization_units 
where kca_operation IN ('INSERT','UPDATE')
group by ORGANIZATION_ID)
);

commit;
 
-- Soft Delete 
update bec_ods.hr_all_organization_units set IS_DELETED_FLG = 'N';
commit;
update bec_ods.hr_all_organization_units set IS_DELETED_FLG = 'Y'
where (ORGANIZATION_ID)  in
(
select ORGANIZATION_ID from bec_raw_dl_ext.hr_all_organization_units
where (ORGANIZATION_ID,KCA_SEQ_ID)
in 
(
select ORGANIZATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.hr_all_organization_units
group by ORGANIZATION_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;


update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='hr_all_organization_units';
commit;

