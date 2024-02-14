DROP table IF EXISTS silver_bec_ods.PER_ALL_PEOPLE_F;
CREATE TABLE IF NOT EXISTS silver_bec_ods.PER_ALL_PEOPLE_F (
  PERSON_ID DECIMAL(10, 0),
  EFFECTIVE_START_DATE TIMESTAMP,
  EFFECTIVE_END_DATE TIMESTAMP,
  BUSINESS_GROUP_ID DECIMAL(15, 0),
  PERSON_TYPE_ID DECIMAL(15, 0),
  LAST_NAME STRING,
  START_DATE TIMESTAMP,
  APPLICANT_NUMBER STRING,
  COMMENT_ID DECIMAL(15, 0),
  CURRENT_APPLICANT_FLAG STRING,
  CURRENT_EMP_OR_APL_FLAG STRING,
  CURRENT_EMPLOYEE_FLAG STRING,
  DATE_EMPLOYEE_DATA_VERIFIED TIMESTAMP,
  DATE_OF_BIRTH TIMESTAMP,
  EMAIL_ADDRESS STRING,
  EMPLOYEE_NUMBER STRING,
  EXPENSE_CHECK_SEND_TO_ADDRESS STRING,
  FIRST_NAME STRING,
  FULL_NAME STRING,
  KNOWN_AS STRING,
  MARITAL_STATUS STRING,
  MIDDLE_NAMES STRING,
  NATIONALITY STRING,
  NATIONAL_IDENTIFIER STRING,
  PREVIOUS_LAST_NAME STRING,
  REGISTERED_DISABLED_FLAG STRING,
  SEX STRING,
  TITLE STRING,
  VENDOR_ID DECIMAL(15, 0),
  WORK_TELEPHONE STRING,
  REQUEST_ID DECIMAL(15, 0),
  PROGRAM_APPLICATION_ID DECIMAL(15, 0),
  PROGRAM_ID DECIMAL(15, 0),
  PROGRAM_UPDATE_DATE TIMESTAMP,
  ATTRIBUTE_CATEGORY STRING,
  ATTRIBUTE1 STRING,
  ATTRIBUTE2 STRING,
  ATTRIBUTE3 STRING,
  ATTRIBUTE4 STRING,
  ATTRIBUTE5 STRING,
  ATTRIBUTE6 STRING,
  ATTRIBUTE7 STRING,
  ATTRIBUTE8 STRING,
  ATTRIBUTE9 STRING,
  ATTRIBUTE10 STRING,
  ATTRIBUTE11 STRING,
  ATTRIBUTE12 STRING,
  ATTRIBUTE13 STRING,
  ATTRIBUTE14 STRING,
  ATTRIBUTE15 STRING,
  ATTRIBUTE16 STRING,
  ATTRIBUTE17 STRING,
  ATTRIBUTE18 STRING,
  ATTRIBUTE19 STRING,
  ATTRIBUTE20 STRING,
  ATTRIBUTE21 STRING,
  ATTRIBUTE22 STRING,
  ATTRIBUTE23 STRING,
  ATTRIBUTE24 STRING,
  ATTRIBUTE25 STRING,
  ATTRIBUTE26 STRING,
  ATTRIBUTE27 STRING,
  ATTRIBUTE28 STRING,
  ATTRIBUTE29 STRING,
  ATTRIBUTE30 STRING,
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  CREATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  PER_INFORMATION_CATEGORY STRING,
  PER_INFORMATION1 STRING,
  PER_INFORMATION2 STRING,
  PER_INFORMATION3 STRING,
  PER_INFORMATION4 STRING,
  PER_INFORMATION5 STRING,
  PER_INFORMATION6 STRING,
  PER_INFORMATION7 STRING,
  PER_INFORMATION8 STRING,
  PER_INFORMATION9 STRING,
  PER_INFORMATION10 STRING,
  PER_INFORMATION11 STRING,
  PER_INFORMATION12 STRING,
  PER_INFORMATION13 STRING,
  PER_INFORMATION14 STRING,
  PER_INFORMATION15 STRING,
  PER_INFORMATION16 STRING,
  PER_INFORMATION17 STRING,
  PER_INFORMATION18 STRING,
  PER_INFORMATION19 STRING,
  PER_INFORMATION20 STRING,
  BACKGROUND_CHECK_STATUS STRING,
  BACKGROUND_DATE_CHECK TIMESTAMP,
  BLOOD_TYPE STRING,
  CORRESPONDENCE_LANGUAGE STRING,
  FAST_PATH_EMPLOYEE STRING,
  FTE_CAPACITY DECIMAL(28, 10),
  HOLD_APPLICANT_DATE_UNTIL TIMESTAMP,
  HONORS STRING,
  INTERNAL_LOCATION STRING,
  LAST_MEDICAL_TEST_BY STRING,
  LAST_MEDICAL_TEST_DATE TIMESTAMP,
  MAILSTOP STRING,
  OFFICE_NUMBER STRING,
  ON_MILITARY_SERVICE STRING,
  ORDER_NAME STRING,
  PRE_NAME_ADJUNCT STRING,
  PROJECTED_START_DATE TIMESTAMP,
  REHIRE_AUTHORIZOR STRING,
  REHIRE_RECOMMENDATION STRING,
  RESUME_EXISTS STRING,
  RESUME_LAST_UPDATED TIMESTAMP,
  SECOND_PASSPORT_EXISTS STRING,
  STUDENT_STATUS STRING,
  SUFFIX STRING,
  WORK_SCHEDULE STRING,
  PER_INFORMATION21 STRING,
  PER_INFORMATION22 STRING,
  PER_INFORMATION23 STRING,
  PER_INFORMATION24 STRING,
  PER_INFORMATION25 STRING,
  PER_INFORMATION26 STRING,
  PER_INFORMATION27 STRING,
  PER_INFORMATION28 STRING,
  PER_INFORMATION29 STRING,
  PER_INFORMATION30 STRING,
  OBJECT_VERSION_NUMBER DECIMAL(9, 0),
  DATE_OF_DEATH TIMESTAMP,
  REHIRE_REASON STRING,
  COORD_BEN_MED_PLN_NO STRING,
  COORD_BEN_NO_CVG_FLAG STRING,
  DPDNT_ADOPTION_DATE TIMESTAMP,
  DPDNT_VLNTRY_SVCE_FLAG STRING,
  RECEIPT_OF_DEATH_CERT_DATE TIMESTAMP,
  USES_TOBACCO_FLAG STRING,
  BENEFIT_GROUP_ID DECIMAL(15, 0),
  ORIGINAL_DATE_OF_HIRE TIMESTAMP,
  TOWN_OF_BIRTH STRING,
  REGION_OF_BIRTH STRING,
  COUNTRY_OF_BIRTH STRING,
  GLOBAL_PERSON_ID STRING,
  COORD_BEN_MED_PL_NAME STRING,
  COORD_BEN_MED_INSR_CRR_NAME STRING,
  COORD_BEN_MED_INSR_CRR_IDENT STRING,
  COORD_BEN_MED_EXT_ER STRING,
  COORD_BEN_MED_CVG_STRT_DT TIMESTAMP,
  COORD_BEN_MED_CVG_END_DT TIMESTAMP,
  PARTY_ID DECIMAL(15, 0),
  NPW_NUMBER STRING,
  CURRENT_NPW_FLAG STRING,
  GLOBAL_NAME STRING,
  LOCAL_NAME STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.PER_ALL_PEOPLE_F (
  PERSON_ID,
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
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    PERSON_ID,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.PER_ALL_PEOPLE_F
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'per_all_people_f';