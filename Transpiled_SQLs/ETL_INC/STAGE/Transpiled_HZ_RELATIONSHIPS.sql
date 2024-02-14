TRUNCATE table bronze_bec_ods_stg.HZ_RELATIONSHIPS;
INSERT INTO bronze_bec_ods_stg.hz_relationships (
  relationship_id,
  subject_id,
  subject_type,
  subject_table_name,
  object_id,
  object_type,
  object_table_name,
  party_id,
  relationship_code,
  directional_flag,
  comments,
  start_date,
  end_date,
  status,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  wh_update_date,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  attribute16,
  attribute17,
  attribute18,
  attribute19,
  attribute20,
  global_attribute_category,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  content_source_type,
  relationship_type,
  object_version_number,
  created_by_module,
  application_id,
  additional_information1,
  additional_information2,
  additional_information3,
  additional_information4,
  additional_information5,
  additional_information6,
  additional_information7,
  additional_information8,
  additional_information9,
  additional_information10,
  additional_information11,
  additional_information12,
  additional_information13,
  additional_information14,
  additional_information15,
  additional_information16,
  additional_information17,
  additional_information18,
  additional_information19,
  additional_information20,
  additional_information21,
  additional_information22,
  additional_information23,
  additional_information24,
  additional_information25,
  additional_information26,
  additional_information27,
  additional_information28,
  additional_information29,
  additional_information30,
  direction_code,
  percentage_ownership,
  actual_content_source,
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
SELECT
  relationship_id,
  subject_id,
  subject_type,
  subject_table_name,
  object_id,
  object_type,
  object_table_name,
  party_id,
  relationship_code,
  directional_flag,
  comments,
  start_date,
  end_date,
  status,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  wh_update_date,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  attribute16,
  attribute17,
  attribute18,
  attribute19,
  attribute20,
  global_attribute_category,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  content_source_type,
  relationship_type,
  object_version_number,
  created_by_module,
  application_id,
  additional_information1,
  additional_information2,
  additional_information3,
  additional_information4,
  additional_information5,
  additional_information6,
  additional_information7,
  additional_information8,
  additional_information9,
  additional_information10,
  additional_information11,
  additional_information12,
  additional_information13,
  additional_information14,
  additional_information15,
  additional_information16,
  additional_information17,
  additional_information18,
  additional_information19,
  additional_information20,
  additional_information21,
  additional_information22,
  additional_information23,
  additional_information24,
  additional_information25,
  additional_information26,
  additional_information27,
  additional_information28,
  additional_information29,
  additional_information30,
  direction_code,
  percentage_ownership,
  actual_content_source,
  KCA_OPERATION,
  KCA_SEQ_ID, /* 'N' as IS_DELETED_FLG, */
  kca_seq_date
FROM bec_raw_dl_ext.hz_relationships
WHERE
  kca_operation <> 'DELETE'
  AND COALESCE(kca_seq_id, '') <> ''
  AND (RELATIONSHIP_ID, DIRECTIONAL_FLAG, kca_seq_id) IN (
    SELECT
      RELATIONSHIP_ID,
      DIRECTIONAL_FLAG,
      MAX(kca_seq_id)
    FROM bec_raw_dl_ext.hz_relationships
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
    GROUP BY
      RELATIONSHIP_ID,
      DIRECTIONAL_FLAG
  )
  AND (
    kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'hz_relationships'
    )
  );