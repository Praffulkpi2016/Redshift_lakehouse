TRUNCATE table bronze_bec_ods_stg.MTL_SERIAL_NUMBERS_TEMP;
INSERT INTO bronze_bec_ods_stg.MTL_SERIAL_NUMBERS_TEMP (
  TRANSACTION_TEMP_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  VENDOR_SERIAL_NUMBER,
  VENDOR_LOT_NUMBER,
  FM_SERIAL_NUMBER,
  TO_SERIAL_NUMBER,
  SERIAL_PREFIX,
  ERROR_CODE,
  PARENT_SERIAL_NUMBER,
  GROUP_HEADER_ID,
  END_ITEM_UNIT_NUMBER,
  SERIAL_ATTRIBUTE_CATEGORY,
  COUNTRY_OF_ORIGIN,
  ORIGINATION_DATE,
  C_ATTRIBUTE1,
  C_ATTRIBUTE2,
  C_ATTRIBUTE3,
  C_ATTRIBUTE4,
  C_ATTRIBUTE5,
  C_ATTRIBUTE6,
  C_ATTRIBUTE7,
  C_ATTRIBUTE8,
  C_ATTRIBUTE9,
  C_ATTRIBUTE10,
  C_ATTRIBUTE11,
  C_ATTRIBUTE12,
  C_ATTRIBUTE13,
  C_ATTRIBUTE14,
  C_ATTRIBUTE15,
  C_ATTRIBUTE16,
  C_ATTRIBUTE17,
  C_ATTRIBUTE18,
  C_ATTRIBUTE19,
  C_ATTRIBUTE20,
  C_ATTRIBUTE21,
  C_ATTRIBUTE22,
  C_ATTRIBUTE23,
  C_ATTRIBUTE24,
  C_ATTRIBUTE25,
  C_ATTRIBUTE26,
  C_ATTRIBUTE27,
  C_ATTRIBUTE28,
  C_ATTRIBUTE29,
  C_ATTRIBUTE30,
  D_ATTRIBUTE1,
  D_ATTRIBUTE2,
  D_ATTRIBUTE3,
  D_ATTRIBUTE4,
  D_ATTRIBUTE5,
  D_ATTRIBUTE6,
  D_ATTRIBUTE7,
  D_ATTRIBUTE8,
  D_ATTRIBUTE9,
  D_ATTRIBUTE10,
  D_ATTRIBUTE11,
  D_ATTRIBUTE12,
  D_ATTRIBUTE13,
  D_ATTRIBUTE14,
  D_ATTRIBUTE15,
  D_ATTRIBUTE16,
  D_ATTRIBUTE17,
  D_ATTRIBUTE18,
  D_ATTRIBUTE19,
  D_ATTRIBUTE20,
  N_ATTRIBUTE1,
  N_ATTRIBUTE2,
  N_ATTRIBUTE3,
  N_ATTRIBUTE4,
  N_ATTRIBUTE5,
  N_ATTRIBUTE6,
  N_ATTRIBUTE7,
  N_ATTRIBUTE8,
  N_ATTRIBUTE9,
  N_ATTRIBUTE10,
  N_ATTRIBUTE11,
  N_ATTRIBUTE12,
  N_ATTRIBUTE13,
  N_ATTRIBUTE14,
  N_ATTRIBUTE15,
  N_ATTRIBUTE16,
  N_ATTRIBUTE17,
  N_ATTRIBUTE18,
  N_ATTRIBUTE19,
  N_ATTRIBUTE20,
  N_ATTRIBUTE21,
  N_ATTRIBUTE22,
  N_ATTRIBUTE23,
  N_ATTRIBUTE24,
  N_ATTRIBUTE25,
  N_ATTRIBUTE26,
  N_ATTRIBUTE27,
  N_ATTRIBUTE28,
  N_ATTRIBUTE29,
  N_ATTRIBUTE30,
  STATUS_ID,
  TERRITORY_CODE,
  TIME_SINCE_NEW,
  CYCLES_SINCE_NEW,
  TIME_SINCE_OVERHAUL,
  CYCLES_SINCE_OVERHAUL,
  TIME_SINCE_REPAIR,
  CYCLES_SINCE_REPAIR,
  TIME_SINCE_VISIT,
  CYCLES_SINCE_VISIT,
  TIME_SINCE_MARK,
  CYCLES_SINCE_MARK,
  NUMER_OF_REPAIRS,
  NUMBER_OF_REPAIRS,
  PRODUCT_CODE,
  PRODUCT_TRANSACTION_ID,
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
  DFF_UPDATED_FLAG,
  PARENT_OBJECT_TYPE,
  PARENT_OBJECT_ID,
  PARENT_OBJECT_NUMBER,
  PARENT_ITEM_ID,
  PARENT_OBJECT_TYPE2,
  PARENT_OBJECT_ID2,
  PARENT_OBJECT_NUMBER2,
  OBJECT_TYPE2,
  OBJECT_NUMBER2,
  LAST_CONSUMED_SERIAL,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    TRANSACTION_TEMP_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    VENDOR_SERIAL_NUMBER,
    VENDOR_LOT_NUMBER,
    FM_SERIAL_NUMBER,
    TO_SERIAL_NUMBER,
    SERIAL_PREFIX,
    ERROR_CODE,
    PARENT_SERIAL_NUMBER,
    GROUP_HEADER_ID,
    END_ITEM_UNIT_NUMBER,
    SERIAL_ATTRIBUTE_CATEGORY,
    COUNTRY_OF_ORIGIN,
    ORIGINATION_DATE,
    C_ATTRIBUTE1,
    C_ATTRIBUTE2,
    C_ATTRIBUTE3,
    C_ATTRIBUTE4,
    C_ATTRIBUTE5,
    C_ATTRIBUTE6,
    C_ATTRIBUTE7,
    C_ATTRIBUTE8,
    C_ATTRIBUTE9,
    C_ATTRIBUTE10,
    C_ATTRIBUTE11,
    C_ATTRIBUTE12,
    C_ATTRIBUTE13,
    C_ATTRIBUTE14,
    C_ATTRIBUTE15,
    C_ATTRIBUTE16,
    C_ATTRIBUTE17,
    C_ATTRIBUTE18,
    C_ATTRIBUTE19,
    C_ATTRIBUTE20,
    C_ATTRIBUTE21,
    C_ATTRIBUTE22,
    C_ATTRIBUTE23,
    C_ATTRIBUTE24,
    C_ATTRIBUTE25,
    C_ATTRIBUTE26,
    C_ATTRIBUTE27,
    C_ATTRIBUTE28,
    C_ATTRIBUTE29,
    C_ATTRIBUTE30,
    D_ATTRIBUTE1,
    D_ATTRIBUTE2,
    D_ATTRIBUTE3,
    D_ATTRIBUTE4,
    D_ATTRIBUTE5,
    D_ATTRIBUTE6,
    D_ATTRIBUTE7,
    D_ATTRIBUTE8,
    D_ATTRIBUTE9,
    D_ATTRIBUTE10,
    D_ATTRIBUTE11,
    D_ATTRIBUTE12,
    D_ATTRIBUTE13,
    D_ATTRIBUTE14,
    D_ATTRIBUTE15,
    D_ATTRIBUTE16,
    D_ATTRIBUTE17,
    D_ATTRIBUTE18,
    D_ATTRIBUTE19,
    D_ATTRIBUTE20,
    N_ATTRIBUTE1,
    N_ATTRIBUTE2,
    N_ATTRIBUTE3,
    N_ATTRIBUTE4,
    N_ATTRIBUTE5,
    N_ATTRIBUTE6,
    N_ATTRIBUTE7,
    N_ATTRIBUTE8,
    N_ATTRIBUTE9,
    N_ATTRIBUTE10,
    N_ATTRIBUTE11,
    N_ATTRIBUTE12,
    N_ATTRIBUTE13,
    N_ATTRIBUTE14,
    N_ATTRIBUTE15,
    N_ATTRIBUTE16,
    N_ATTRIBUTE17,
    N_ATTRIBUTE18,
    N_ATTRIBUTE19,
    N_ATTRIBUTE20,
    N_ATTRIBUTE21,
    N_ATTRIBUTE22,
    N_ATTRIBUTE23,
    N_ATTRIBUTE24,
    N_ATTRIBUTE25,
    N_ATTRIBUTE26,
    N_ATTRIBUTE27,
    N_ATTRIBUTE28,
    N_ATTRIBUTE29,
    N_ATTRIBUTE30,
    STATUS_ID,
    TERRITORY_CODE,
    TIME_SINCE_NEW,
    CYCLES_SINCE_NEW,
    TIME_SINCE_OVERHAUL,
    CYCLES_SINCE_OVERHAUL,
    TIME_SINCE_REPAIR,
    CYCLES_SINCE_REPAIR,
    TIME_SINCE_VISIT,
    CYCLES_SINCE_VISIT,
    TIME_SINCE_MARK,
    CYCLES_SINCE_MARK,
    NUMER_OF_REPAIRS,
    NUMBER_OF_REPAIRS,
    PRODUCT_CODE,
    PRODUCT_TRANSACTION_ID,
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
    DFF_UPDATED_FLAG,
    PARENT_OBJECT_TYPE,
    PARENT_OBJECT_ID,
    PARENT_OBJECT_NUMBER,
    PARENT_ITEM_ID,
    PARENT_OBJECT_TYPE2,
    PARENT_OBJECT_ID2,
    PARENT_OBJECT_NUMBER2,
    OBJECT_TYPE2,
    OBJECT_NUMBER2,
    LAST_CONSUMED_SERIAL,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_SERIAL_NUMBERS_TEMP
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (TRANSACTION_TEMP_ID, FM_SERIAL_NUMBER, kca_seq_id) IN (
      SELECT
        TRANSACTION_TEMP_ID,
        FM_SERIAL_NUMBER,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_SERIAL_NUMBERS_TEMP
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        TRANSACTION_TEMP_ID,
        FM_SERIAL_NUMBER
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mtl_serial_numbers_temp'
    )
);