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

delete from bec_ods.per_all_people_f
where (PERSON_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE) in (
select stg.PERSON_ID, stg.EFFECTIVE_START_DATE, stg.EFFECTIVE_END_DATE from bec_ods.per_all_people_f ods, bec_ods_stg.per_all_people_f stg
where ods.PERSON_ID = stg.PERSON_ID and ods.EFFECTIVE_START_DATE = stg.EFFECTIVE_START_DATE and ods.EFFECTIVE_END_DATE = stg.EFFECTIVE_END_DATE 
and stg.kca_operation in ('INSERT','UPDATE'));

commit;


-- Insert records

insert into bec_ods.per_all_people_f
(PERSON_ID
,EFFECTIVE_START_DATE
,EFFECTIVE_END_DATE
,BUSINESS_GROUP_ID
,PERSON_TYPE_ID
,LAST_NAME
,START_DATE
,APPLICANT_NUMBER
,COMMENT_ID
,CURRENT_APPLICANT_FLAG
,CURRENT_EMP_OR_APL_FLAG
,CURRENT_EMPLOYEE_FLAG
,DATE_EMPLOYEE_DATA_VERIFIED
,DATE_OF_BIRTH
,EMAIL_ADDRESS
,EMPLOYEE_NUMBER
,EXPENSE_CHECK_SEND_TO_ADDRESS
,FIRST_NAME
,FULL_NAME
,KNOWN_AS
,MARITAL_STATUS
,MIDDLE_NAMES
,NATIONALITY
,NATIONAL_IDENTIFIER
,PREVIOUS_LAST_NAME
,REGISTERED_DISABLED_FLAG
,SEX
,TITLE
,VENDOR_ID
,WORK_TELEPHONE
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,ATTRIBUTE16
,ATTRIBUTE17
,ATTRIBUTE18
,ATTRIBUTE19
,ATTRIBUTE20
,ATTRIBUTE21
,ATTRIBUTE22
,ATTRIBUTE23
,ATTRIBUTE24
,ATTRIBUTE25
,ATTRIBUTE26
,ATTRIBUTE27
,ATTRIBUTE28
,ATTRIBUTE29
,ATTRIBUTE30
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,CREATED_BY
,CREATION_DATE
,PER_INFORMATION_CATEGORY
,PER_INFORMATION1
,PER_INFORMATION2
,PER_INFORMATION3
,PER_INFORMATION4
,PER_INFORMATION5
,PER_INFORMATION6
,PER_INFORMATION7
,PER_INFORMATION8
,PER_INFORMATION9
,PER_INFORMATION10
,PER_INFORMATION11
,PER_INFORMATION12
,PER_INFORMATION13
,PER_INFORMATION14
,PER_INFORMATION15
,PER_INFORMATION16
,PER_INFORMATION17
,PER_INFORMATION18
,PER_INFORMATION19
,PER_INFORMATION20
,BACKGROUND_CHECK_STATUS
,BACKGROUND_DATE_CHECK
,BLOOD_TYPE
,CORRESPONDENCE_LANGUAGE
,FAST_PATH_EMPLOYEE
,FTE_CAPACITY
,HOLD_APPLICANT_DATE_UNTIL
,HONORS
,INTERNAL_LOCATION
,LAST_MEDICAL_TEST_BY
,LAST_MEDICAL_TEST_DATE
,MAILSTOP
,OFFICE_NUMBER
,ON_MILITARY_SERVICE
,ORDER_NAME
,PRE_NAME_ADJUNCT
,PROJECTED_START_DATE
,REHIRE_AUTHORIZOR
,REHIRE_RECOMMENDATION
,RESUME_EXISTS
,RESUME_LAST_UPDATED
,SECOND_PASSPORT_EXISTS
,STUDENT_STATUS
,SUFFIX
,WORK_SCHEDULE
,PER_INFORMATION21
,PER_INFORMATION22
,PER_INFORMATION23
,PER_INFORMATION24
,PER_INFORMATION25
,PER_INFORMATION26
,PER_INFORMATION27
,PER_INFORMATION28
,PER_INFORMATION29
,PER_INFORMATION30
,OBJECT_VERSION_NUMBER
,DATE_OF_DEATH
,REHIRE_REASON
,COORD_BEN_MED_PLN_NO
,COORD_BEN_NO_CVG_FLAG
,DPDNT_ADOPTION_DATE
,DPDNT_VLNTRY_SVCE_FLAG
,RECEIPT_OF_DEATH_CERT_DATE
,USES_TOBACCO_FLAG
,BENEFIT_GROUP_ID
,ORIGINAL_DATE_OF_HIRE
,TOWN_OF_BIRTH
,REGION_OF_BIRTH
,COUNTRY_OF_BIRTH
,GLOBAL_PERSON_ID
,COORD_BEN_MED_PL_NAME
,COORD_BEN_MED_INSR_CRR_NAME
,COORD_BEN_MED_INSR_CRR_IDENT
,COORD_BEN_MED_EXT_ER
,COORD_BEN_MED_CVG_STRT_DT
,COORD_BEN_MED_CVG_END_DT
,PARTY_ID
,NPW_NUMBER
,CURRENT_NPW_FLAG
,GLOBAL_NAME
,LOCAL_NAME
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(
select PERSON_ID,
EFFECTIVE_START_DATE,
EFFECTIVE_END_DATE,
BUSINESS_GROUP_ID,
PERSON_TYPE_ID,
LAST_NAME,
START_DATE,
APPLICANT_NUMBER,
COMMENT_ID,
CURRENT_APPLICANT_FLAG,
CURRENT_EMP_OR_APL_FLAG,
CURRENT_EMPLOYEE_FLAG,
DATE_EMPLOYEE_DATA_VERIFIED,
DATE_OF_BIRTH,
EMAIL_ADDRESS,
EMPLOYEE_NUMBER,
EXPENSE_CHECK_SEND_TO_ADDRESS,
FIRST_NAME,
FULL_NAME,
KNOWN_AS,
MARITAL_STATUS,
MIDDLE_NAMES,
NATIONALITY,
NATIONAL_IDENTIFIER,
PREVIOUS_LAST_NAME,
REGISTERED_DISABLED_FLAG,
SEX,
TITLE,
VENDOR_ID,
WORK_TELEPHONE,
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
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
CREATED_BY,
CREATION_DATE,
PER_INFORMATION_CATEGORY,
PER_INFORMATION1,
PER_INFORMATION2,
PER_INFORMATION3,
PER_INFORMATION4,
PER_INFORMATION5,
PER_INFORMATION6,
PER_INFORMATION7,
PER_INFORMATION8,
PER_INFORMATION9,
PER_INFORMATION10,
PER_INFORMATION11,
PER_INFORMATION12,
PER_INFORMATION13,
PER_INFORMATION14,
PER_INFORMATION15,
PER_INFORMATION16,
PER_INFORMATION17,
PER_INFORMATION18,
PER_INFORMATION19,
PER_INFORMATION20,
BACKGROUND_CHECK_STATUS,
BACKGROUND_DATE_CHECK,
BLOOD_TYPE,
CORRESPONDENCE_LANGUAGE,
FAST_PATH_EMPLOYEE,
FTE_CAPACITY,
HOLD_APPLICANT_DATE_UNTIL,
HONORS,
INTERNAL_LOCATION,
LAST_MEDICAL_TEST_BY,
LAST_MEDICAL_TEST_DATE,
MAILSTOP,
OFFICE_NUMBER,
ON_MILITARY_SERVICE,
ORDER_NAME,
PRE_NAME_ADJUNCT,
PROJECTED_START_DATE,
REHIRE_AUTHORIZOR,
REHIRE_RECOMMENDATION,
RESUME_EXISTS,
RESUME_LAST_UPDATED,
SECOND_PASSPORT_EXISTS,
STUDENT_STATUS,
SUFFIX,
WORK_SCHEDULE,
PER_INFORMATION21,
PER_INFORMATION22,
PER_INFORMATION23,
PER_INFORMATION24,
PER_INFORMATION25,
PER_INFORMATION26,
PER_INFORMATION27,
PER_INFORMATION28,
PER_INFORMATION29,
PER_INFORMATION30,
OBJECT_VERSION_NUMBER,
DATE_OF_DEATH,
REHIRE_REASON,
COORD_BEN_MED_PLN_NO,
COORD_BEN_NO_CVG_FLAG,
DPDNT_ADOPTION_DATE,
DPDNT_VLNTRY_SVCE_FLAG,
RECEIPT_OF_DEATH_CERT_DATE,
USES_TOBACCO_FLAG,
BENEFIT_GROUP_ID,
ORIGINAL_DATE_OF_HIRE,
TOWN_OF_BIRTH,
REGION_OF_BIRTH,
COUNTRY_OF_BIRTH,
GLOBAL_PERSON_ID,
COORD_BEN_MED_PL_NAME,
COORD_BEN_MED_INSR_CRR_NAME,
COORD_BEN_MED_INSR_CRR_IDENT,
COORD_BEN_MED_EXT_ER,
COORD_BEN_MED_CVG_STRT_DT,
COORD_BEN_MED_CVG_END_DT,
PARTY_ID,
NPW_NUMBER,
CURRENT_NPW_FLAG,
GLOBAL_NAME,
LOCAL_NAME,
KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
  KCA_SEQ_DATE
from bec_ods_stg.per_all_people_f
where kca_operation in ('INSERT','UPDATE') and (PERSON_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE,kca_seq_id) in 
(select PERSON_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE,max(kca_seq_id) from bec_ods_stg.per_all_people_f 
where kca_operation in ('INSERT','UPDATE')
group by PERSON_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE)
);

commit;


-- Soft delete
update bec_ods.per_all_people_f set IS_DELETED_FLG = 'N';
commit;
update bec_ods.per_all_people_f set IS_DELETED_FLG = 'Y'
where (PERSON_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE)  in
(
select PERSON_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE from bec_raw_dl_ext.per_all_people_f
where (PERSON_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE,KCA_SEQ_ID)
in 
(
select PERSON_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.per_all_people_f
group by PERSON_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', last_refresh_date = getdate() where ods_table_name='per_all_people_f';
commit;