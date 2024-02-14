/* Delete Records */
DELETE FROM silver_bec_ods.hr_organization_information
WHERE
  ORG_INFORMATION_ID IN (
    SELECT
      stg.ORG_INFORMATION_ID
    FROM silver_bec_ods.hr_organization_information AS ods, bronze_bec_ods_stg.hr_organization_information AS stg
    WHERE
      ods.ORG_INFORMATION_ID = stg.ORG_INFORMATION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
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
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (ORG_INFORMATION_ID, kca_seq_id) IN (
      SELECT
        ORG_INFORMATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.hr_organization_information
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ORG_INFORMATION_ID
    )
);
/* Soft Delete */
UPDATE silver_bec_ods.hr_organization_information SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.hr_organization_information SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ORG_INFORMATION_ID
  ) IN (
    SELECT
      ORG_INFORMATION_ID
    FROM bec_raw_dl_ext.hr_organization_information
    WHERE
      (ORG_INFORMATION_ID, KCA_SEQ_ID) IN (
        SELECT
          ORG_INFORMATION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.hr_organization_information
        GROUP BY
          ORG_INFORMATION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hr_organization_information';