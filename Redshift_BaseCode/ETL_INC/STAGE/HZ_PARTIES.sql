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

truncate table bec_ods_stg.hz_parties;

insert
	into
	bec_ods_stg.hz_parties
(PARTY_ID,
PARTY_NUMBER,
PARTY_NAME,
PARTY_TYPE,
VALIDATED_FLAG,
LAST_UPDATED_BY,
CREATION_DATE,
LAST_UPDATE_LOGIN,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
CREATED_BY,
LAST_UPDATE_DATE,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
WH_UPDATE_DATE,
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
GLOBAL_ATTRIBUTE_CATEGORY,
GLOBAL_ATTRIBUTE1,
GLOBAL_ATTRIBUTE2,
GLOBAL_ATTRIBUTE4,
GLOBAL_ATTRIBUTE3,
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
ORIG_SYSTEM_REFERENCE,
SIC_CODE,
HQ_BRANCH_IND,
CUSTOMER_KEY,
TAX_REFERENCE,
JGZZ_FISCAL_CODE,
DUNS_NUMBER,
TAX_NAME,
PERSON_PRE_NAME_ADJUNCT,
PERSON_FIRST_NAME,
PERSON_MIDDLE_NAME,
PERSON_LAST_NAME,
PERSON_NAME_SUFFIX,
PERSON_TITLE,
PERSON_ACADEMIC_TITLE,
PERSON_PREVIOUS_LAST_NAME,
KNOWN_AS,
PERSON_IDEN_TYPE,
PERSON_IDENTIFIER,
GROUP_TYPE,
COUNTRY,
ADDRESS1,
ADDRESS2,
ADDRESS3,
ADDRESS4,
CITY,
POSTAL_CODE,
STATE,
PROVINCE,
STATUS,
COUNTY,
SIC_CODE_TYPE,
TOTAL_NUM_OF_ORDERS,
TOTAL_ORDERED_AMOUNT,
LAST_ORDERED_DATE,
URL,
EMAIL_ADDRESS,
DO_NOT_MAIL_FLAG,
ANALYSIS_FY,
FISCAL_YEAREND_MONTH,
EMPLOYEES_TOTAL,
CURR_FY_POTENTIAL_REVENUE,
NEXT_FY_POTENTIAL_REVENUE,
YEAR_ESTABLISHED,
GSA_INDICATOR_FLAG,
MISSION_STATEMENT,
ORGANIZATION_NAME_PHONETIC,
PERSON_FIRST_NAME_PHONETIC,
PERSON_LAST_NAME_PHONETIC,
LANGUAGE_NAME,
CATEGORY_CODE,
REFERENCE_USE_FLAG,
THIRD_PARTY_FLAG,
COMPETITOR_FLAG,
SALUTATION,
KNOWN_AS2,
KNOWN_AS3,
KNOWN_AS4,
KNOWN_AS5,
DUNS_NUMBER_C,
OBJECT_VERSION_NUMBER,
CREATED_BY_MODULE,
APPLICATION_ID,
CERTIFICATION_LEVEL,
CERT_REASON_CODE,
PRIMARY_PHONE_CONTACT_PT_ID,
PRIMARY_PHONE_PURPOSE,
PRIMARY_PHONE_LINE_TYPE,
PRIMARY_PHONE_COUNTRY_CODE,
PRIMARY_PHONE_AREA_CODE,
PRIMARY_PHONE_NUMBER,
PRIMARY_PHONE_EXTENSION,
PREFERRED_CONTACT_METHOD,
HOME_COUNTRY,
PERSON_BO_VERSION,
ORG_BO_VERSION,
PERSON_CUST_BO_VERSION,
ORG_CUST_BO_VERSION,
KCA_OPERATION,
kca_seq_id,
	kca_seq_date)
(
	select
	PARTY_ID,
PARTY_NUMBER,
PARTY_NAME,
PARTY_TYPE,
VALIDATED_FLAG,
LAST_UPDATED_BY,
CREATION_DATE,
LAST_UPDATE_LOGIN,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
CREATED_BY,
LAST_UPDATE_DATE,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
WH_UPDATE_DATE,
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
GLOBAL_ATTRIBUTE_CATEGORY,
GLOBAL_ATTRIBUTE1,
GLOBAL_ATTRIBUTE2,
GLOBAL_ATTRIBUTE4,
GLOBAL_ATTRIBUTE3,
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
ORIG_SYSTEM_REFERENCE,
SIC_CODE,
HQ_BRANCH_IND,
CUSTOMER_KEY,
TAX_REFERENCE,
JGZZ_FISCAL_CODE,
DUNS_NUMBER,
TAX_NAME,
PERSON_PRE_NAME_ADJUNCT,
PERSON_FIRST_NAME,
PERSON_MIDDLE_NAME,
PERSON_LAST_NAME,
PERSON_NAME_SUFFIX,
PERSON_TITLE,
PERSON_ACADEMIC_TITLE,
PERSON_PREVIOUS_LAST_NAME,
KNOWN_AS,
PERSON_IDEN_TYPE,
PERSON_IDENTIFIER,
GROUP_TYPE,
COUNTRY,
ADDRESS1,
ADDRESS2,
ADDRESS3,
ADDRESS4,
CITY,
POSTAL_CODE,
STATE,
PROVINCE,
STATUS,
COUNTY,
SIC_CODE_TYPE,
TOTAL_NUM_OF_ORDERS,
TOTAL_ORDERED_AMOUNT,
LAST_ORDERED_DATE,
URL,
EMAIL_ADDRESS,
DO_NOT_MAIL_FLAG,
ANALYSIS_FY,
FISCAL_YEAREND_MONTH,
EMPLOYEES_TOTAL,
CURR_FY_POTENTIAL_REVENUE,
NEXT_FY_POTENTIAL_REVENUE,
YEAR_ESTABLISHED,
GSA_INDICATOR_FLAG,
MISSION_STATEMENT,
ORGANIZATION_NAME_PHONETIC,
PERSON_FIRST_NAME_PHONETIC,
PERSON_LAST_NAME_PHONETIC,
LANGUAGE_NAME,
CATEGORY_CODE,
REFERENCE_USE_FLAG,
THIRD_PARTY_FLAG,
COMPETITOR_FLAG,
SALUTATION,
KNOWN_AS2,
KNOWN_AS3,
KNOWN_AS4,
KNOWN_AS5,
DUNS_NUMBER_C,
OBJECT_VERSION_NUMBER,
CREATED_BY_MODULE,
APPLICATION_ID,
CERTIFICATION_LEVEL,
CERT_REASON_CODE,
PRIMARY_PHONE_CONTACT_PT_ID,
PRIMARY_PHONE_PURPOSE,
PRIMARY_PHONE_LINE_TYPE,
PRIMARY_PHONE_COUNTRY_CODE,
PRIMARY_PHONE_AREA_CODE,
PRIMARY_PHONE_NUMBER,
PRIMARY_PHONE_EXTENSION,
PREFERRED_CONTACT_METHOD,
HOME_COUNTRY,
PERSON_BO_VERSION,
ORG_BO_VERSION,
PERSON_CUST_BO_VERSION,
ORG_CUST_BO_VERSION,
KCA_OPERATION
,kca_seq_id,
	kca_seq_date
from bec_raw_dl_ext.hz_parties	
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' and (party_id,kca_seq_id) in (select party_id,max(kca_seq_id) from bec_raw_dl_ext.hz_parties 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by party_id)
and 
( kca_seq_date > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'hz_parties')
  
            )
);
end;

