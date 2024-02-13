/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/
begin;
drop table if exists bec_ods.PA_TASKS;

CREATE TABLE IF NOT EXISTS bec_ods.PA_TASKS
(
TASK_ID	NUMERIC(15,0)   ENCODE az64
,PROJECT_ID	NUMERIC(15,0)   ENCODE az64
,TASK_NUMBER	VARCHAR(25)   ENCODE lzo
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,TASK_NAME	VARCHAR(20)   ENCODE lzo
,TOP_TASK_ID	NUMERIC(15,0)   ENCODE az64
,WBS_LEVEL	NUMERIC(15,0)   ENCODE az64
,READY_TO_BILL_FLAG	VARCHAR(1)   ENCODE lzo
,READY_TO_DISTRIBUTE_FLAG	VARCHAR(1)   ENCODE lzo
,PARENT_TASK_ID	NUMERIC(15,0)   ENCODE az64
,DESCRIPTION	VARCHAR(250)   ENCODE lzo
,CARRYING_OUT_ORGANIZATION_ID	NUMERIC(15,0)   ENCODE az64
,SERVICE_TYPE_CODE	VARCHAR(30)   ENCODE lzo
,TASK_MANAGER_PERSON_ID	NUMERIC(15,0)   ENCODE az64
,CHARGEABLE_FLAG	VARCHAR(1)   ENCODE lzo
,BILLABLE_FLAG	VARCHAR(1)   ENCODE lzo
,LIMIT_TO_TXN_CONTROLS_FLAG	VARCHAR(1)   ENCODE lzo
,START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,COMPLETION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ADDRESS_ID	NUMERIC(15,0)   ENCODE az64
,LABOR_BILL_RATE_ORG_ID	NUMERIC(15,0)   ENCODE az64
,LABOR_STD_BILL_RATE_SCHDL	VARCHAR(20)   ENCODE lzo
,LABOR_SCHEDULE_FIXED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LABOR_SCHEDULE_DISCOUNT	NUMERIC(28,10)   ENCODE az64
,NON_LABOR_BILL_RATE_ORG_ID	NUMERIC(15,0)   ENCODE az64
,NON_LABOR_STD_BILL_RATE_SCHDL	VARCHAR(30)   ENCODE lzo
,NON_LABOR_SCHEDULE_FIXED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,NON_LABOR_SCHEDULE_DISCOUNT	NUMERIC(28,10)   ENCODE az64
,LABOR_COST_MULTIPLIER_NAME	VARCHAR(20)   ENCODE lzo
,REQUEST_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_APPLICATION_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ATTRIBUTE_CATEGORY	VARCHAR(30)   ENCODE lzo
,ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE9	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE10	VARCHAR(150)   ENCODE lzo
,COST_IND_RATE_SCH_ID	NUMERIC(15,0)   ENCODE az64
,REV_IND_RATE_SCH_ID	NUMERIC(15,0)   ENCODE az64
,INV_IND_RATE_SCH_ID	NUMERIC(15,0)   ENCODE az64
,COST_IND_SCH_FIXED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,REV_IND_SCH_FIXED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,INV_IND_SCH_FIXED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LABOR_SCH_TYPE	VARCHAR(1)   ENCODE lzo
,NON_LABOR_SCH_TYPE	VARCHAR(1)   ENCODE lzo
,OVR_COST_IND_RATE_SCH_ID	NUMERIC(15,0)   ENCODE az64
,OVR_INV_IND_RATE_SCH_ID	NUMERIC(15,0)   ENCODE az64
,OVR_REV_IND_RATE_SCH_ID	NUMERIC(15,0)   ENCODE az64
,PM_PRODUCT_CODE	VARCHAR(30)   ENCODE lzo
,PM_TASK_REFERENCE	VARCHAR(25)   ENCODE lzo
,ACTUAL_START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ACTUAL_FINISH_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,EARLY_START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,EARLY_FINISH_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LATE_START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LATE_FINISH_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,SCHEDULED_START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,SCHEDULED_FINISH_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ADW_NOTIFY_FLAG	VARCHAR(1)   ENCODE lzo
,ALLOW_CROSS_CHARGE_FLAG	VARCHAR(1)   ENCODE lzo
,PROJECT_RATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,PROJECT_RATE_TYPE	VARCHAR(30)   ENCODE lzo
,CC_PROCESS_LABOR_FLAG	VARCHAR(1)   ENCODE lzo
,LABOR_TP_SCHEDULE_ID	NUMERIC(15,0)   ENCODE az64
,LABOR_TP_FIXED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CC_PROCESS_NL_FLAG	VARCHAR(1)   ENCODE lzo
,NL_TP_SCHEDULE_ID	NUMERIC(15,0)   ENCODE az64
,NL_TP_FIXED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,RECEIVE_PROJECT_INVOICE_FLAG	VARCHAR(1)   ENCODE lzo
,WORK_TYPE_ID	NUMERIC(15,0)   ENCODE az64
,RECORD_VERSION_NUMBER	NUMERIC(15,0)   ENCODE az64
,JOB_BILL_RATE_SCHEDULE_ID	NUMERIC(15,0)   ENCODE az64
,EMP_BILL_RATE_SCHEDULE_ID	NUMERIC(15,0)   ENCODE az64
,TASKFUNC_COST_RATE_TYPE	VARCHAR(30)   ENCODE lzo
,TASKFUNC_COST_RATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,NON_LAB_STD_BILL_RT_SCH_ID	NUMERIC(15,0)   ENCODE az64
,LABOR_DISC_REASON_CODE	VARCHAR(30)   ENCODE lzo
,NON_LABOR_DISC_REASON_CODE	VARCHAR(30)   ENCODE lzo
,LONG_TASK_NAME	VARCHAR(240)   ENCODE lzo
,RETIREMENT_COST_FLAG	VARCHAR(1)   ENCODE lzo
,CINT_ELIGIBLE_FLAG	VARCHAR(1)   ENCODE lzo
,CINT_STOP_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,REVENUE_ACCRUAL_METHOD	VARCHAR(30)   ENCODE lzo
,INVOICE_METHOD	VARCHAR(30)   ENCODE lzo
,CUSTOMER_ID	NUMERIC(15,0)   ENCODE az64
,GEN_ETC_SOURCE_CODE	VARCHAR(30)   ENCODE lzo
,BILL_SCHE_OVRD_FLAG	VARCHAR(1)   ENCODE lzo
,ADJ_ON_STD_INV VARCHAR(2)   ENCODE lzo
,ALT_LIQUIDATION_RATE_PER NUMERIC(17,2)   ENCODE az64
,BILL_GROUP_ID NUMERIC(15,0)   ENCODE az64
,COST_PLUS_FIXED_FEE_FLAG VARCHAR(1)   ENCODE lzo
,FEE_PERCENTAGE NUMERIC(17,2)   ENCODE az64
,FEE_RATE NUMERIC(25,5)   ENCODE az64
,FEE_RATE_SCHEDULE_ID NUMERIC(15,0)   ENCODE az64
,FEE_REVENUE_SCHEDULE_ID NUMERIC(15,0)   ENCODE az64
,IC_LABOR_TP_FIXED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,IC_LABOR_TP_SCHEDULE_ID NUMERIC(22,0)   ENCODE az64
,IC_NL_TP_FIXED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,IC_NL_TP_SCHEDULE_ID NUMERIC(22,0)   ENCODE az64
,INV_LINE_GROUP VARCHAR(30)   ENCODE lzo
,LIQUIDATION_RATE_PERCENTAGE NUMERIC(17,2)   ENCODE az64
,OVR_FEE_RATE_SCHEDULE_ID NUMERIC(15,0)   ENCODE az64
,PROGRESS_PAYMENT_FLAG VARCHAR(1)   ENCODE lzo
,RATE_PERCENTAGE NUMERIC(17,2)   ENCODE az64
,RETENTION_PERCENTAGE NUMERIC(17,2)   ENCODE az64
,SUBCONTRACT_FLAG VARCHAR(1)   ENCODE lzo
,WITHHOLD_AMOUNT NUMERIC(22,5)   ENCODE az64
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)

DISTSTYLE AUTO
;

insert into bec_ods.PA_TASKS
(TASK_ID
,PROJECT_ID
,TASK_NUMBER
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,TASK_NAME
,TOP_TASK_ID
,WBS_LEVEL
,READY_TO_BILL_FLAG
,READY_TO_DISTRIBUTE_FLAG
,PARENT_TASK_ID
,DESCRIPTION
,CARRYING_OUT_ORGANIZATION_ID
,SERVICE_TYPE_CODE
,TASK_MANAGER_PERSON_ID
,CHARGEABLE_FLAG
,BILLABLE_FLAG
,LIMIT_TO_TXN_CONTROLS_FLAG
,START_DATE
,COMPLETION_DATE
,ADDRESS_ID
,LABOR_BILL_RATE_ORG_ID
,LABOR_STD_BILL_RATE_SCHDL
,LABOR_SCHEDULE_FIXED_DATE
,LABOR_SCHEDULE_DISCOUNT
,NON_LABOR_BILL_RATE_ORG_ID
,NON_LABOR_STD_BILL_RATE_SCHDL
,NON_LABOR_SCHEDULE_FIXED_DATE
,NON_LABOR_SCHEDULE_DISCOUNT
,LABOR_COST_MULTIPLIER_NAME
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
,COST_IND_RATE_SCH_ID
,REV_IND_RATE_SCH_ID
,INV_IND_RATE_SCH_ID
,COST_IND_SCH_FIXED_DATE
,REV_IND_SCH_FIXED_DATE
,INV_IND_SCH_FIXED_DATE
,LABOR_SCH_TYPE
,NON_LABOR_SCH_TYPE
,OVR_COST_IND_RATE_SCH_ID
,OVR_INV_IND_RATE_SCH_ID
,OVR_REV_IND_RATE_SCH_ID
,PM_PRODUCT_CODE
,PM_TASK_REFERENCE
,ACTUAL_START_DATE
,ACTUAL_FINISH_DATE
,EARLY_START_DATE
,EARLY_FINISH_DATE
,LATE_START_DATE
,LATE_FINISH_DATE
,SCHEDULED_START_DATE
,SCHEDULED_FINISH_DATE
,ADW_NOTIFY_FLAG
,ALLOW_CROSS_CHARGE_FLAG
,PROJECT_RATE_DATE
,PROJECT_RATE_TYPE
,CC_PROCESS_LABOR_FLAG
,LABOR_TP_SCHEDULE_ID
,LABOR_TP_FIXED_DATE
,CC_PROCESS_NL_FLAG
,NL_TP_SCHEDULE_ID
,NL_TP_FIXED_DATE
,RECEIVE_PROJECT_INVOICE_FLAG
,WORK_TYPE_ID
,RECORD_VERSION_NUMBER
,JOB_BILL_RATE_SCHEDULE_ID
,EMP_BILL_RATE_SCHEDULE_ID
,TASKFUNC_COST_RATE_TYPE
,TASKFUNC_COST_RATE_DATE
,NON_LAB_STD_BILL_RT_SCH_ID
,LABOR_DISC_REASON_CODE
,NON_LABOR_DISC_REASON_CODE
,LONG_TASK_NAME
,RETIREMENT_COST_FLAG
,CINT_ELIGIBLE_FLAG
,CINT_STOP_DATE
,REVENUE_ACCRUAL_METHOD
,INVOICE_METHOD
,CUSTOMER_ID
,GEN_ETC_SOURCE_CODE
,BILL_SCHE_OVRD_FLAG
,ADJ_ON_STD_INV
,ALT_LIQUIDATION_RATE_PER
,BILL_GROUP_ID
,COST_PLUS_FIXED_FEE_FLAG
,FEE_PERCENTAGE
,FEE_RATE
,FEE_RATE_SCHEDULE_ID
,FEE_REVENUE_SCHEDULE_ID
,IC_LABOR_TP_FIXED_DATE
,IC_LABOR_TP_SCHEDULE_ID
,IC_NL_TP_FIXED_DATE
,IC_NL_TP_SCHEDULE_ID
,INV_LINE_GROUP
,LIQUIDATION_RATE_PERCENTAGE
,OVR_FEE_RATE_SCHEDULE_ID
,PROGRESS_PAYMENT_FLAG
,RATE_PERCENTAGE
,RETENTION_PERCENTAGE
,SUBCONTRACT_FLAG
,WITHHOLD_AMOUNT
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(
select
TASK_ID
,PROJECT_ID
,TASK_NUMBER
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,TASK_NAME
,TOP_TASK_ID
,WBS_LEVEL
,READY_TO_BILL_FLAG
,READY_TO_DISTRIBUTE_FLAG
,PARENT_TASK_ID
,DESCRIPTION
,CARRYING_OUT_ORGANIZATION_ID
,SERVICE_TYPE_CODE
,TASK_MANAGER_PERSON_ID
,CHARGEABLE_FLAG
,BILLABLE_FLAG
,LIMIT_TO_TXN_CONTROLS_FLAG
,START_DATE
,COMPLETION_DATE
,ADDRESS_ID
,LABOR_BILL_RATE_ORG_ID
,LABOR_STD_BILL_RATE_SCHDL
,LABOR_SCHEDULE_FIXED_DATE
,LABOR_SCHEDULE_DISCOUNT
,NON_LABOR_BILL_RATE_ORG_ID
,NON_LABOR_STD_BILL_RATE_SCHDL
,NON_LABOR_SCHEDULE_FIXED_DATE
,NON_LABOR_SCHEDULE_DISCOUNT
,LABOR_COST_MULTIPLIER_NAME
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
,COST_IND_RATE_SCH_ID
,REV_IND_RATE_SCH_ID
,INV_IND_RATE_SCH_ID
,COST_IND_SCH_FIXED_DATE
,REV_IND_SCH_FIXED_DATE
,INV_IND_SCH_FIXED_DATE
,LABOR_SCH_TYPE
,NON_LABOR_SCH_TYPE
,OVR_COST_IND_RATE_SCH_ID
,OVR_INV_IND_RATE_SCH_ID
,OVR_REV_IND_RATE_SCH_ID
,PM_PRODUCT_CODE
,PM_TASK_REFERENCE
,ACTUAL_START_DATE
,ACTUAL_FINISH_DATE
,EARLY_START_DATE
,EARLY_FINISH_DATE
,LATE_START_DATE
,LATE_FINISH_DATE
,SCHEDULED_START_DATE
,SCHEDULED_FINISH_DATE
,ADW_NOTIFY_FLAG
,ALLOW_CROSS_CHARGE_FLAG
,PROJECT_RATE_DATE
,PROJECT_RATE_TYPE
,CC_PROCESS_LABOR_FLAG
,LABOR_TP_SCHEDULE_ID
,LABOR_TP_FIXED_DATE
,CC_PROCESS_NL_FLAG
,NL_TP_SCHEDULE_ID
,NL_TP_FIXED_DATE
,RECEIVE_PROJECT_INVOICE_FLAG
,WORK_TYPE_ID
,RECORD_VERSION_NUMBER
,JOB_BILL_RATE_SCHEDULE_ID
,EMP_BILL_RATE_SCHEDULE_ID
,TASKFUNC_COST_RATE_TYPE
,TASKFUNC_COST_RATE_DATE
,NON_LAB_STD_BILL_RT_SCH_ID
,LABOR_DISC_REASON_CODE
,NON_LABOR_DISC_REASON_CODE
,LONG_TASK_NAME
,RETIREMENT_COST_FLAG
,CINT_ELIGIBLE_FLAG
,CINT_STOP_DATE
,REVENUE_ACCRUAL_METHOD
,INVOICE_METHOD
,CUSTOMER_ID
,GEN_ETC_SOURCE_CODE
,BILL_SCHE_OVRD_FLAG
,ADJ_ON_STD_INV
,ALT_LIQUIDATION_RATE_PER
,BILL_GROUP_ID
,COST_PLUS_FIXED_FEE_FLAG
,FEE_PERCENTAGE
,FEE_RATE
,FEE_RATE_SCHEDULE_ID
,FEE_REVENUE_SCHEDULE_ID
,IC_LABOR_TP_FIXED_DATE
,IC_LABOR_TP_SCHEDULE_ID
,IC_NL_TP_FIXED_DATE
,IC_NL_TP_SCHEDULE_ID
,INV_LINE_GROUP
,LIQUIDATION_RATE_PERCENTAGE
,OVR_FEE_RATE_SCHEDULE_ID
,PROGRESS_PAYMENT_FLAG
,RATE_PERCENTAGE
,RETENTION_PERCENTAGE
,SUBCONTRACT_FLAG
,WITHHOLD_AMOUNT
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID 
,kca_seq_date
from bec_ods_stg.PA_TASKS);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='pa_tasks'; 

commit;