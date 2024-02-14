DROP TABLE IF EXISTS silver_bec_ods.wsh_carriers;
CREATE TABLE IF NOT EXISTS silver_bec_ods.wsh_carriers (
  CARRIER_ID DECIMAL(15, 0),
  FREIGHT_CODE STRING,
  SCAC_CODE STRING,
  MANIFESTING_ENABLED_FLAG STRING,
  CURRENCY_CODE STRING,
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
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  GENERIC_FLAG STRING,
  FREIGHT_BILL_AUTO_APPROVAL STRING,
  FREIGHT_AUDIT_LINE_LEVEL STRING,
  SUPPLIER_ID DECIMAL(15, 0),
  SUPPLIER_SITE_ID DECIMAL(15, 0),
  MAX_NUM_STOPS_PERMITTED DECIMAL(28, 10),
  MAX_TOTAL_DISTANCE DECIMAL(28, 10),
  MAX_TOTAL_TIME DECIMAL(28, 10),
  ALLOW_INTERSPERSE_LOAD STRING,
  MIN_LAYOVER_TIME DECIMAL(28, 10),
  MAX_LAYOVER_TIME DECIMAL(28, 10),
  MAX_TOTAL_DISTANCE_IN_24HR DECIMAL(28, 10),
  MAX_DRIVING_TIME_IN_24HR DECIMAL(28, 10),
  MAX_DUTY_TIME_IN_24HR DECIMAL(28, 10),
  ALLOW_CONTINUOUS_MOVE STRING,
  MAX_CM_DISTANCE DECIMAL(28, 10),
  MAX_CM_TIME DECIMAL(28, 10),
  MAX_CM_DH_DISTANCE DECIMAL(28, 10),
  MAX_CM_DH_TIME DECIMAL(28, 10),
  MIN_CM_DISTANCE DECIMAL(28, 10),
  MIN_CM_TIME DECIMAL(28, 10),
  CM_FREE_DH_MILEAGE DECIMAL(28, 10),
  CM_FIRST_LOAD_DISCOUNT STRING,
  CM_RATE_VARIANT STRING,
  MAX_SIZE_WIDTH DECIMAL(28, 10),
  MAX_SIZE_HEIGHT DECIMAL(28, 10),
  MAX_SIZE_LENGTH DECIMAL(28, 10),
  MIN_SIZE_WIDTH DECIMAL(28, 10),
  MIN_SIZE_HEIGHT DECIMAL(28, 10),
  MIN_SIZE_LENGTH DECIMAL(28, 10),
  TIME_UOM STRING,
  DIMENSION_UOM STRING,
  DISTANCE_UOM STRING,
  WEIGHT_UOM STRING,
  VOLUME_UOM STRING,
  MAX_OUT_OF_ROUTE DECIMAL(28, 10),
  DISTANCE_CALCULATION_METHOD STRING,
  ORIGIN_DSTN_SURCHARGE_LEVEL STRING,
  UNIT_RATE_BASIS STRING,
  DIM_DIMENSIONAL_FACTOR DECIMAL(28, 10),
  DIM_WEIGHT_UOM STRING,
  DIM_VOLUME_UOM STRING,
  DIM_DIMENSION_UOM STRING,
  DIM_MIN_PACK_VOL DECIMAL(28, 10),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.wsh_carriers (
  CARRIER_ID,
  FREIGHT_CODE,
  SCAC_CODE,
  MANIFESTING_ENABLED_FLAG,
  CURRENCY_CODE,
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
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  GENERIC_FLAG,
  FREIGHT_BILL_AUTO_APPROVAL,
  FREIGHT_AUDIT_LINE_LEVEL,
  SUPPLIER_ID,
  SUPPLIER_SITE_ID,
  MAX_NUM_STOPS_PERMITTED,
  MAX_TOTAL_DISTANCE,
  MAX_TOTAL_TIME,
  ALLOW_INTERSPERSE_LOAD,
  MIN_LAYOVER_TIME,
  MAX_LAYOVER_TIME,
  MAX_TOTAL_DISTANCE_IN_24HR,
  MAX_DRIVING_TIME_IN_24HR,
  MAX_DUTY_TIME_IN_24HR,
  ALLOW_CONTINUOUS_MOVE,
  MAX_CM_DISTANCE,
  MAX_CM_TIME,
  MAX_CM_DH_DISTANCE,
  MAX_CM_DH_TIME,
  MIN_CM_DISTANCE,
  MIN_CM_TIME,
  CM_FREE_DH_MILEAGE,
  CM_FIRST_LOAD_DISCOUNT,
  CM_RATE_VARIANT,
  MAX_SIZE_WIDTH,
  MAX_SIZE_HEIGHT,
  MAX_SIZE_LENGTH,
  MIN_SIZE_WIDTH,
  MIN_SIZE_HEIGHT,
  MIN_SIZE_LENGTH,
  TIME_UOM,
  DIMENSION_UOM,
  DISTANCE_UOM,
  WEIGHT_UOM,
  VOLUME_UOM,
  MAX_OUT_OF_ROUTE,
  DISTANCE_CALCULATION_METHOD,
  ORIGIN_DSTN_SURCHARGE_LEVEL,
  UNIT_RATE_BASIS,
  DIM_DIMENSIONAL_FACTOR,
  DIM_WEIGHT_UOM,
  DIM_VOLUME_UOM,
  DIM_DIMENSION_UOM,
  DIM_MIN_PACK_VOL,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  CARRIER_ID,
  FREIGHT_CODE,
  SCAC_CODE,
  MANIFESTING_ENABLED_FLAG,
  CURRENCY_CODE,
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
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  GENERIC_FLAG,
  FREIGHT_BILL_AUTO_APPROVAL,
  FREIGHT_AUDIT_LINE_LEVEL,
  SUPPLIER_ID,
  SUPPLIER_SITE_ID,
  MAX_NUM_STOPS_PERMITTED,
  MAX_TOTAL_DISTANCE,
  MAX_TOTAL_TIME,
  ALLOW_INTERSPERSE_LOAD,
  MIN_LAYOVER_TIME,
  MAX_LAYOVER_TIME,
  MAX_TOTAL_DISTANCE_IN_24HR,
  MAX_DRIVING_TIME_IN_24HR,
  MAX_DUTY_TIME_IN_24HR,
  ALLOW_CONTINUOUS_MOVE,
  MAX_CM_DISTANCE,
  MAX_CM_TIME,
  MAX_CM_DH_DISTANCE,
  MAX_CM_DH_TIME,
  MIN_CM_DISTANCE,
  MIN_CM_TIME,
  CM_FREE_DH_MILEAGE,
  CM_FIRST_LOAD_DISCOUNT,
  CM_RATE_VARIANT,
  MAX_SIZE_WIDTH,
  MAX_SIZE_HEIGHT,
  MAX_SIZE_LENGTH,
  MIN_SIZE_WIDTH,
  MIN_SIZE_HEIGHT,
  MIN_SIZE_LENGTH,
  TIME_UOM,
  DIMENSION_UOM,
  DISTANCE_UOM,
  WEIGHT_UOM,
  VOLUME_UOM,
  MAX_OUT_OF_ROUTE,
  DISTANCE_CALCULATION_METHOD,
  ORIGIN_DSTN_SURCHARGE_LEVEL,
  UNIT_RATE_BASIS,
  DIM_DIMENSIONAL_FACTOR,
  DIM_WEIGHT_UOM,
  DIM_VOLUME_UOM,
  DIM_DIMENSION_UOM,
  DIM_MIN_PACK_VOL,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.wsh_carriers;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wsh_carriers';