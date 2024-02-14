DROP TABLE IF EXISTS silver_bec_ods.JTF_TASKS_B;
CREATE TABLE IF NOT EXISTS silver_bec_ods.JTF_TASKS_B (
  TASK_ID DECIMAL(15, 0),
  CREATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  OBJECT_VERSION_NUMBER DECIMAL(15, 0),
  TASK_NUMBER STRING,
  TASK_TYPE_ID DECIMAL(15, 0),
  TASK_STATUS_ID DECIMAL(15, 0),
  TASK_PRIORITY_ID DECIMAL(15, 0),
  OWNER_ID DECIMAL(15, 0),
  OWNER_TYPE_CODE STRING,
  OWNER_TERRITORY_ID DECIMAL(15, 0),
  ASSIGNED_BY_ID DECIMAL(15, 0),
  CUST_ACCOUNT_ID DECIMAL(15, 0),
  CUSTOMER_ID DECIMAL(15, 0),
  ADDRESS_ID DECIMAL(15, 0),
  PLANNED_START_DATE TIMESTAMP,
  PLANNED_END_DATE TIMESTAMP,
  SCHEDULED_START_DATE TIMESTAMP,
  SCHEDULED_END_DATE TIMESTAMP,
  ACTUAL_START_DATE TIMESTAMP,
  ACTUAL_END_DATE TIMESTAMP,
  SOURCE_OBJECT_TYPE_CODE STRING,
  TIMEZONE_ID DECIMAL(15, 0),
  SOURCE_OBJECT_ID DECIMAL(15, 0),
  SOURCE_OBJECT_NAME STRING,
  DURATION DECIMAL(28, 10),
  DURATION_UOM STRING,
  PLANNED_EFFORT DECIMAL(28, 10),
  PLANNED_EFFORT_UOM STRING,
  ACTUAL_EFFORT DECIMAL(28, 10),
  ACTUAL_EFFORT_UOM STRING,
  PERCENTAGE_COMPLETE DECIMAL(28, 10),
  REASON_CODE STRING,
  PRIVATE_FLAG STRING,
  PUBLISH_FLAG STRING,
  RESTRICT_CLOSURE_FLAG STRING,
  MULTI_BOOKED_FLAG STRING,
  MILESTONE_FLAG STRING,
  HOLIDAY_FLAG STRING,
  BILLABLE_FLAG STRING,
  BOUND_MODE_CODE STRING,
  SOFT_BOUND_FLAG STRING,
  WORKFLOW_PROCESS_ID DECIMAL(15, 0),
  NOTIFICATION_FLAG STRING,
  NOTIFICATION_PERIOD DECIMAL(28, 10),
  NOTIFICATION_PERIOD_UOM STRING,
  PARENT_TASK_ID DECIMAL(15, 0),
  RECURRENCE_RULE_ID DECIMAL(15, 0),
  ALARM_START DECIMAL(28, 10),
  ALARM_START_UOM STRING,
  ALARM_ON STRING,
  ALARM_COUNT DECIMAL(28, 10),
  ALARM_FIRED_COUNT DECIMAL(28, 10),
  ALARM_INTERVAL DECIMAL(28, 10),
  ALARM_INTERVAL_UOM STRING,
  DELETED_FLAG STRING,
  PALM_FLAG STRING,
  WINCE_FLAG STRING,
  LAPTOP_FLAG STRING,
  DEVICE1_FLAG STRING,
  DEVICE2_FLAG STRING,
  DEVICE3_FLAG STRING,
  COSTS DECIMAL(28, 10),
  CURRENCY_CODE STRING,
  ORG_ID DECIMAL(15, 0),
  ESCALATION_LEVEL STRING,
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
  ATTRIBUTE_CATEGORY STRING,
  SECURITY_GROUP_ID DECIMAL(15, 0),
  ORIG_SYSTEM_REFERENCE STRING,
  ORIG_SYSTEM_REFERENCE_ID DECIMAL(15, 0),
  UPDATE_STATUS_FLAG STRING,
  CALENDAR_START_DATE TIMESTAMP,
  CALENDAR_END_DATE TIMESTAMP,
  DATE_SELECTED STRING,
  TEMPLATE_ID DECIMAL(15, 0),
  TEMPLATE_GROUP_ID DECIMAL(15, 0),
  OBJECT_CHANGED_DATE TIMESTAMP,
  TASK_CONFIRMATION_STATUS STRING,
  TASK_CONFIRMATION_COUNTER DECIMAL(28, 10),
  TASK_SPLIT_FLAG STRING,
  OPEN_FLAG STRING,
  ENTITY STRING,
  CHILD_POSITION STRING,
  CHILD_SEQUENCE_NUM DECIMAL(15, 0),
  LOCATION_ID DECIMAL(15, 0),
  CRITICAL_TASK_FLAG STRING,
  CUST_IMP_LEVEL DECIMAL(15, 0),
  FOLLOW_UP_TASK_FLAG STRING,
  BASE_TASK_ID DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.JTF_TASKS_B (
  TASK_ID,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  OBJECT_VERSION_NUMBER,
  TASK_NUMBER,
  TASK_TYPE_ID,
  TASK_STATUS_ID,
  TASK_PRIORITY_ID,
  OWNER_ID,
  OWNER_TYPE_CODE,
  OWNER_TERRITORY_ID,
  ASSIGNED_BY_ID,
  CUST_ACCOUNT_ID,
  CUSTOMER_ID,
  ADDRESS_ID,
  PLANNED_START_DATE,
  PLANNED_END_DATE,
  SCHEDULED_START_DATE,
  SCHEDULED_END_DATE,
  ACTUAL_START_DATE,
  ACTUAL_END_DATE,
  SOURCE_OBJECT_TYPE_CODE,
  TIMEZONE_ID,
  SOURCE_OBJECT_ID,
  SOURCE_OBJECT_NAME,
  DURATION,
  DURATION_UOM,
  PLANNED_EFFORT,
  PLANNED_EFFORT_UOM,
  ACTUAL_EFFORT,
  ACTUAL_EFFORT_UOM,
  PERCENTAGE_COMPLETE,
  REASON_CODE,
  PRIVATE_FLAG,
  PUBLISH_FLAG,
  RESTRICT_CLOSURE_FLAG,
  MULTI_BOOKED_FLAG,
  MILESTONE_FLAG,
  HOLIDAY_FLAG,
  BILLABLE_FLAG,
  BOUND_MODE_CODE,
  SOFT_BOUND_FLAG,
  WORKFLOW_PROCESS_ID,
  NOTIFICATION_FLAG,
  NOTIFICATION_PERIOD,
  NOTIFICATION_PERIOD_UOM,
  PARENT_TASK_ID,
  RECURRENCE_RULE_ID,
  ALARM_START,
  ALARM_START_UOM,
  ALARM_ON,
  ALARM_COUNT,
  ALARM_FIRED_COUNT,
  ALARM_INTERVAL,
  ALARM_INTERVAL_UOM,
  DELETED_FLAG,
  PALM_FLAG,
  WINCE_FLAG,
  LAPTOP_FLAG,
  DEVICE1_FLAG,
  DEVICE2_FLAG,
  DEVICE3_FLAG,
  COSTS,
  CURRENCY_CODE,
  ORG_ID,
  ESCALATION_LEVEL,
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
  ATTRIBUTE_CATEGORY,
  SECURITY_GROUP_ID,
  ORIG_SYSTEM_REFERENCE,
  ORIG_SYSTEM_REFERENCE_ID,
  UPDATE_STATUS_FLAG,
  CALENDAR_START_DATE,
  CALENDAR_END_DATE,
  DATE_SELECTED,
  TEMPLATE_ID,
  TEMPLATE_GROUP_ID,
  OBJECT_CHANGED_DATE,
  TASK_CONFIRMATION_STATUS,
  TASK_CONFIRMATION_COUNTER,
  TASK_SPLIT_FLAG,
  OPEN_FLAG,
  ENTITY,
  CHILD_POSITION,
  CHILD_SEQUENCE_NUM,
  LOCATION_ID,
  CRITICAL_TASK_FLAG,
  CUST_IMP_LEVEL,
  FOLLOW_UP_TASK_FLAG,
  BASE_TASK_ID,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  TASK_ID,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  OBJECT_VERSION_NUMBER,
  TASK_NUMBER,
  TASK_TYPE_ID,
  TASK_STATUS_ID,
  TASK_PRIORITY_ID,
  OWNER_ID,
  OWNER_TYPE_CODE,
  OWNER_TERRITORY_ID,
  ASSIGNED_BY_ID,
  CUST_ACCOUNT_ID,
  CUSTOMER_ID,
  ADDRESS_ID,
  PLANNED_START_DATE,
  PLANNED_END_DATE,
  SCHEDULED_START_DATE,
  SCHEDULED_END_DATE,
  ACTUAL_START_DATE,
  ACTUAL_END_DATE,
  SOURCE_OBJECT_TYPE_CODE,
  TIMEZONE_ID,
  SOURCE_OBJECT_ID,
  SOURCE_OBJECT_NAME,
  DURATION,
  DURATION_UOM,
  PLANNED_EFFORT,
  PLANNED_EFFORT_UOM,
  ACTUAL_EFFORT,
  ACTUAL_EFFORT_UOM,
  PERCENTAGE_COMPLETE,
  REASON_CODE,
  PRIVATE_FLAG,
  PUBLISH_FLAG,
  RESTRICT_CLOSURE_FLAG,
  MULTI_BOOKED_FLAG,
  MILESTONE_FLAG,
  HOLIDAY_FLAG,
  BILLABLE_FLAG,
  BOUND_MODE_CODE,
  SOFT_BOUND_FLAG,
  WORKFLOW_PROCESS_ID,
  NOTIFICATION_FLAG,
  NOTIFICATION_PERIOD,
  NOTIFICATION_PERIOD_UOM,
  PARENT_TASK_ID,
  RECURRENCE_RULE_ID,
  ALARM_START,
  ALARM_START_UOM,
  ALARM_ON,
  ALARM_COUNT,
  ALARM_FIRED_COUNT,
  ALARM_INTERVAL,
  ALARM_INTERVAL_UOM,
  DELETED_FLAG,
  PALM_FLAG,
  WINCE_FLAG,
  LAPTOP_FLAG,
  DEVICE1_FLAG,
  DEVICE2_FLAG,
  DEVICE3_FLAG,
  COSTS,
  CURRENCY_CODE,
  ORG_ID,
  ESCALATION_LEVEL,
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
  ATTRIBUTE_CATEGORY,
  SECURITY_GROUP_ID,
  ORIG_SYSTEM_REFERENCE,
  ORIG_SYSTEM_REFERENCE_ID,
  UPDATE_STATUS_FLAG,
  CALENDAR_START_DATE,
  CALENDAR_END_DATE,
  DATE_SELECTED,
  TEMPLATE_ID,
  TEMPLATE_GROUP_ID,
  OBJECT_CHANGED_DATE,
  TASK_CONFIRMATION_STATUS,
  TASK_CONFIRMATION_COUNTER,
  TASK_SPLIT_FLAG,
  OPEN_FLAG,
  ENTITY,
  CHILD_POSITION,
  CHILD_SEQUENCE_NUM,
  LOCATION_ID,
  CRITICAL_TASK_FLAG,
  CUST_IMP_LEVEL,
  FOLLOW_UP_TASK_FLAG,
  BASE_TASK_ID,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.JTF_TASKS_B;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'jtf_tasks_b';