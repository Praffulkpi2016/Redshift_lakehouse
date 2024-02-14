DROP table IF EXISTS silver_bec_ods.HR_ORGANIZATION_INFORMATION;
CREATE TABLE IF NOT EXISTS silver_bec_ods.HR_ORGANIZATION_INFORMATION (
  ORG_INFORMATION_ID DECIMAL(15, 0),
  ORG_INFORMATION_CONTEXT STRING,
  ORGANIZATION_ID DECIMAL(15, 0),
  ORG_INFORMATION1 STRING,
  ORG_INFORMATION10 STRING,
  ORG_INFORMATION11 STRING,
  ORG_INFORMATION12 STRING,
  ORG_INFORMATION13 STRING,
  ORG_INFORMATION14 STRING,
  ORG_INFORMATION15 STRING,
  ORG_INFORMATION16 STRING,
  ORG_INFORMATION17 STRING,
  ORG_INFORMATION18 STRING,
  ORG_INFORMATION19 STRING,
  ORG_INFORMATION2 STRING,
  ORG_INFORMATION20 STRING,
  ORG_INFORMATION3 STRING,
  ORG_INFORMATION4 STRING,
  ORG_INFORMATION5 STRING,
  ORG_INFORMATION6 STRING,
  ORG_INFORMATION7 STRING,
  ORG_INFORMATION8 STRING,
  ORG_INFORMATION9 STRING,
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
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  CREATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  OBJECT_VERSION_NUMBER DECIMAL(9, 0),
  PARTY_ID DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.hr_organization_information
(
  SELECT
    ORG_INFORMATION_ID,
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
    PARTY_ID,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.hr_organization_information
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hr_organization_information';