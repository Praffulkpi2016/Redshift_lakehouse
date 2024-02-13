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

delete from bec_ods.hr_organization_information
where ORG_INFORMATION_ID in (
select stg.ORG_INFORMATION_ID from bec_ods.hr_organization_information ods, bec_ods_stg.hr_organization_information stg
where ods.ORG_INFORMATION_ID = stg.ORG_INFORMATION_ID  and stg.kca_operation IN ('INSERT','UPDATE') );

commit; 
-- Insert records
insert into bec_ods.hr_organization_information
(
select ORG_INFORMATION_ID,
ORG_INFORMATION_CONTEXT,
ORGANIZATION_ID,
ORG_INFORMATION1,
ORG_INFORMATION10,
ORG_INFORMATION11,
ORG_INFORMATION12,
ORG_INFORMATION13,
ORG_INFORMATION14,
ORG_INFORMATION15,
ORG_INFORMATION16,
ORG_INFORMATION17,
ORG_INFORMATION18,
ORG_INFORMATION19,
ORG_INFORMATION2,
ORG_INFORMATION20,
ORG_INFORMATION3,
ORG_INFORMATION4,
ORG_INFORMATION5,
ORG_INFORMATION6,
ORG_INFORMATION7,
ORG_INFORMATION8,
ORG_INFORMATION9,
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
PARTY_ID
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID ,
	kca_seq_date
from bec_ods_stg.hr_organization_information
where kca_operation IN ('INSERT','UPDATE') and (ORG_INFORMATION_ID,kca_seq_id) in (select ORG_INFORMATION_ID,max(kca_seq_id) from bec_ods_stg.hr_organization_information 
where kca_operation IN ('INSERT','UPDATE')
group by ORG_INFORMATION_ID)
);

commit; 
-- Soft Delete
update bec_ods.hr_organization_information set IS_DELETED_FLG = 'N';
commit;
update bec_ods.hr_organization_information set IS_DELETED_FLG = 'Y'
where (ORG_INFORMATION_ID)  in
(
select ORG_INFORMATION_ID from bec_raw_dl_ext.hr_organization_information
where (ORG_INFORMATION_ID,KCA_SEQ_ID)
in 
(
select ORG_INFORMATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.hr_organization_information
group by ORG_INFORMATION_ID
) 
and kca_operation= 'DELETE'
);
commit;
end; 

update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='hr_organization_information';
commit;

