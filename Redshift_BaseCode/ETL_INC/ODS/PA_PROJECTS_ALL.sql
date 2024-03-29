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

delete from bec_ods.pa_projects_all
where PROJECT_ID in (
select stg.PROJECT_ID from bec_ods.pa_projects_all ods, bec_ods_stg.pa_projects_all stg
where ods.PROJECT_ID = stg.PROJECT_ID and stg.kca_operation in ('INSERT','UPDATE'));

commit;


-- Insert records

insert into bec_ods.pa_projects_all
(PROJECT_ID
,NAME
,SEGMENT1
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,PROJECT_TYPE
,CARRYING_OUT_ORGANIZATION_ID
,PUBLIC_SECTOR_FLAG
,PROJECT_STATUS_CODE
,DESCRIPTION
,START_DATE
,COMPLETION_DATE
,CLOSED_DATE
,DISTRIBUTION_RULE
,LABOR_INVOICE_FORMAT_ID
,NON_LABOR_INVOICE_FORMAT_ID
,RETENTION_INVOICE_FORMAT_ID
,RETENTION_PERCENTAGE
,BILLING_OFFSET
,BILLING_CYCLE
,LABOR_STD_BILL_RATE_SCHDL
,LABOR_BILL_RATE_ORG_ID
,LABOR_SCHEDULE_FIXED_DATE
,LABOR_SCHEDULE_DISCOUNT
,NON_LABOR_STD_BILL_RATE_SCHDL
,NON_LABOR_BILL_RATE_ORG_ID
,NON_LABOR_SCHEDULE_FIXED_DATE
,NON_LABOR_SCHEDULE_DISCOUNT
,LIMIT_TO_TXN_CONTROLS_FLAG
,PROJECT_LEVEL_FUNDING_FLAG
,INVOICE_COMMENT
,UNBILLED_RECEIVABLE_DR
,UNEARNED_REVENUE_CR
,REQUEST_ID
,PROGRAM_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_UPDATE_DATE
,SUMMARY_FLAG
,ENABLED_FLAG
,SEGMENT2
,SEGMENT3
,SEGMENT4
,SEGMENT5
,SEGMENT6
,SEGMENT7
,SEGMENT8
,SEGMENT9
,SEGMENT10
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
,OVR_REV_IND_RATE_SCH_ID
,OVR_INV_IND_RATE_SCH_ID
,TEMPLATE_FLAG
,VERIFICATION_DATE
,CREATED_FROM_PROJECT_ID
,TEMPLATE_START_DATE_ACTIVE
,TEMPLATE_END_DATE_ACTIVE
,ORG_ID
,PM_PRODUCT_CODE
,PM_PROJECT_REFERENCE
,ACTUAL_START_DATE
,ACTUAL_FINISH_DATE
,EARLY_START_DATE
,EARLY_FINISH_DATE
,LATE_START_DATE
,LATE_FINISH_DATE
,SCHEDULED_START_DATE
,SCHEDULED_FINISH_DATE
,BILLING_CYCLE_ID
,ADW_NOTIFY_FLAG
,WF_STATUS_CODE
,OUTPUT_TAX_CODE
,RETENTION_TAX_CODE
,PROJECT_CURRENCY_CODE
,ALLOW_CROSS_CHARGE_FLAG
,PROJECT_RATE_DATE
,PROJECT_RATE_TYPE
,CC_PROCESS_LABOR_FLAG
,LABOR_TP_SCHEDULE_ID
,LABOR_TP_FIXED_DATE
,CC_PROCESS_NL_FLAG
,NL_TP_SCHEDULE_ID
,NL_TP_FIXED_DATE
,CC_TAX_TASK_ID
,BILL_JOB_GROUP_ID
,COST_JOB_GROUP_ID
,ROLE_LIST_ID
,WORK_TYPE_ID
,CALENDAR_ID
,LOCATION_ID
,PROBABILITY_MEMBER_ID
,PROJECT_VALUE
,EXPECTED_APPROVAL_DATE
,RECORD_VERSION_NUMBER
,INITIAL_TEAM_TEMPLATE_ID
,JOB_BILL_RATE_SCHEDULE_ID
,EMP_BILL_RATE_SCHEDULE_ID
,COMPETENCE_MATCH_WT
,AVAILABILITY_MATCH_WT
,JOB_LEVEL_MATCH_WT
,ENABLE_AUTOMATED_SEARCH
,SEARCH_MIN_AVAILABILITY
,SEARCH_ORG_HIER_ID
,SEARCH_STARTING_ORG_ID
,SEARCH_COUNTRY_CODE
,MIN_CAND_SCORE_REQD_FOR_NOM
,NON_LAB_STD_BILL_RT_SCH_ID
,INVPROC_CURRENCY_TYPE
,REVPROC_CURRENCY_CODE
,PROJECT_BIL_RATE_DATE_CODE
,PROJECT_BIL_RATE_TYPE
,PROJECT_BIL_RATE_DATE
,PROJECT_BIL_EXCHANGE_RATE
,PROJFUNC_CURRENCY_CODE
,PROJFUNC_BIL_RATE_DATE_CODE
,PROJFUNC_BIL_RATE_TYPE
,PROJFUNC_BIL_RATE_DATE
,PROJFUNC_BIL_EXCHANGE_RATE
,FUNDING_RATE_DATE_CODE
,FUNDING_RATE_TYPE
,FUNDING_RATE_DATE
,FUNDING_EXCHANGE_RATE
,BASELINE_FUNDING_FLAG
,PROJFUNC_COST_RATE_TYPE
,PROJFUNC_COST_RATE_DATE
,INV_BY_BILL_TRANS_CURR_FLAG
,MULTI_CURRENCY_BILLING_FLAG
,SPLIT_COST_FROM_WORKPLAN_FLAG
,SPLIT_COST_FROM_BILL_FLAG
,ASSIGN_PRECEDES_TASK
,PRIORITY_CODE
,RETN_BILLING_INV_FORMAT_ID
,RETN_ACCOUNTING_FLAG
,ADV_ACTION_SET_ID
,START_ADV_ACTION_SET_FLAG
,REVALUATE_FUNDING_FLAG
,INCLUDE_GAINS_LOSSES_FLAG
,TARGET_START_DATE
,TARGET_FINISH_DATE
,BASELINE_START_DATE
,BASELINE_FINISH_DATE
,SCHEDULED_AS_OF_DATE
,BASELINE_AS_OF_DATE
,LABOR_DISC_REASON_CODE
,NON_LABOR_DISC_REASON_CODE
,SECURITY_LEVEL
,ACTUAL_AS_OF_DATE
,SCHEDULED_DURATION
,BASELINE_DURATION
,ACTUAL_DURATION
,LONG_NAME
,BTC_COST_BASE_REV_CODE
,ASSET_ALLOCATION_METHOD
,CAPITAL_EVENT_PROCESSING
,CINT_RATE_SCH_ID
,CINT_ELIGIBLE_FLAG
,CINT_STOP_DATE
,SYS_PROGRAM_FLAG
,STRUCTURE_SHARING_CODE
,ENABLE_TOP_TASK_CUSTOMER_FLAG
,ENABLE_TOP_TASK_INV_MTH_FLAG
,REVENUE_ACCRUAL_METHOD
,INVOICE_METHOD
,PROJFUNC_ATTR_FOR_AR_FLAG
,PJI_SOURCE_FLAG
,ALLOW_MULTI_PROGRAM_ROLLUP
,PROJ_REQ_RES_FORMAT_ID
,PROJ_ASGMT_RES_FORMAT_ID
,FUNDING_APPROVAL_STATUS_CODE
,REVTRANS_CURRENCY_TYPE
,DATE_EFF_FUNDS_CONSUMPTION
,AR_REC_NOTIFY_FLAG
,AUTO_RELEASE_PWP_INV
,ADJ_ON_STD_INV
,ALT_LIQUIDATION_RATE_PER
,BILL_GROUP_ID
,BILL_LABOR_ACCRUAL
,BILLING_FUNDS_CHECK_FC_FLAG
,CBS_ENABLE_FLAG
,CBS_VERSION_ID
,CONTRACT_ID
,COST_PLUS_FIXED_FEE_FLAG
,DEFERRED_ROLLUP_ENABLED_FLAG
,DERIVE_BILLING_FROM_OKE_FLAG
,IC_LABOR_TP_FIXED_DATE
,IC_LABOR_TP_SCHEDULE_ID
,IC_NL_TP_FIXED_DATE
,IC_NL_TP_SCHEDULE_ID
,FEE_PERCENTAGE
,FEE_RATE
,FEE_RATE_SCHEDULE_ID
,FEE_REVENUE_SCHEDULE_ID
,INV_LINE_GROUP
,LABOR_FEE_RATE_CURRENCY_CODE
,LAST_SUMMARIZATION_DATE
,LIQUIDATION_RATE_PERCENTAGE
,OLAP_GROUP
,OLAP_TASK_ID
,OVR_FEE_RATE_SCHEDULE_ID
,PJT_ROLLUP_ENABLED_FLAG
,POBG_FLAG
,PROGRESS_PAYMENT_FLAG
,PROJ_SOV_FRM_CNTRT_FLAG
,PROJ_SOV_LEVEL
,RATE_PERCENTAGE
,RMCS_INTEGRATION_FLAG
,SLA_ACC_END_DATE
,WITHHOLD_AMOUNT
,WP_BASELINE_VER_FLAG
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(
select PROJECT_ID,
NAME,
SEGMENT1,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
PROJECT_TYPE,
CARRYING_OUT_ORGANIZATION_ID,
PUBLIC_SECTOR_FLAG,
PROJECT_STATUS_CODE,
DESCRIPTION,
START_DATE,
COMPLETION_DATE,
CLOSED_DATE,
DISTRIBUTION_RULE,
LABOR_INVOICE_FORMAT_ID,
NON_LABOR_INVOICE_FORMAT_ID,
RETENTION_INVOICE_FORMAT_ID,
RETENTION_PERCENTAGE,
BILLING_OFFSET,
BILLING_CYCLE,
LABOR_STD_BILL_RATE_SCHDL,
LABOR_BILL_RATE_ORG_ID,
LABOR_SCHEDULE_FIXED_DATE,
LABOR_SCHEDULE_DISCOUNT,
NON_LABOR_STD_BILL_RATE_SCHDL,
NON_LABOR_BILL_RATE_ORG_ID,
NON_LABOR_SCHEDULE_FIXED_DATE,
NON_LABOR_SCHEDULE_DISCOUNT,
LIMIT_TO_TXN_CONTROLS_FLAG,
PROJECT_LEVEL_FUNDING_FLAG,
INVOICE_COMMENT,
UNBILLED_RECEIVABLE_DR,
UNEARNED_REVENUE_CR,
REQUEST_ID,
PROGRAM_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_UPDATE_DATE,
SUMMARY_FLAG,
ENABLED_FLAG,
SEGMENT2,
SEGMENT3,
SEGMENT4,
SEGMENT5,
SEGMENT6,
SEGMENT7,
SEGMENT8,
SEGMENT9,
SEGMENT10,
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
COST_IND_RATE_SCH_ID,
REV_IND_RATE_SCH_ID,
INV_IND_RATE_SCH_ID,
COST_IND_SCH_FIXED_DATE,
REV_IND_SCH_FIXED_DATE,
INV_IND_SCH_FIXED_DATE,
LABOR_SCH_TYPE,
NON_LABOR_SCH_TYPE,
OVR_COST_IND_RATE_SCH_ID,
OVR_REV_IND_RATE_SCH_ID,
OVR_INV_IND_RATE_SCH_ID,
TEMPLATE_FLAG,
VERIFICATION_DATE,
CREATED_FROM_PROJECT_ID,
TEMPLATE_START_DATE_ACTIVE,
TEMPLATE_END_DATE_ACTIVE,
ORG_ID,
PM_PRODUCT_CODE,
PM_PROJECT_REFERENCE,
ACTUAL_START_DATE,
ACTUAL_FINISH_DATE,
EARLY_START_DATE,
EARLY_FINISH_DATE,
LATE_START_DATE,
LATE_FINISH_DATE,
SCHEDULED_START_DATE,
SCHEDULED_FINISH_DATE,
BILLING_CYCLE_ID,
ADW_NOTIFY_FLAG,
WF_STATUS_CODE,
OUTPUT_TAX_CODE,
RETENTION_TAX_CODE,
PROJECT_CURRENCY_CODE,
ALLOW_CROSS_CHARGE_FLAG,
PROJECT_RATE_DATE,
PROJECT_RATE_TYPE,
CC_PROCESS_LABOR_FLAG,
LABOR_TP_SCHEDULE_ID,
LABOR_TP_FIXED_DATE,
CC_PROCESS_NL_FLAG,
NL_TP_SCHEDULE_ID,
NL_TP_FIXED_DATE,
CC_TAX_TASK_ID,
BILL_JOB_GROUP_ID,
COST_JOB_GROUP_ID,
ROLE_LIST_ID,
WORK_TYPE_ID,
CALENDAR_ID,
LOCATION_ID,
PROBABILITY_MEMBER_ID,
PROJECT_VALUE,
EXPECTED_APPROVAL_DATE,
RECORD_VERSION_NUMBER,
INITIAL_TEAM_TEMPLATE_ID,
JOB_BILL_RATE_SCHEDULE_ID,
EMP_BILL_RATE_SCHEDULE_ID,
COMPETENCE_MATCH_WT,
AVAILABILITY_MATCH_WT,
JOB_LEVEL_MATCH_WT,
ENABLE_AUTOMATED_SEARCH,
SEARCH_MIN_AVAILABILITY,
SEARCH_ORG_HIER_ID,
SEARCH_STARTING_ORG_ID,
SEARCH_COUNTRY_CODE,
MIN_CAND_SCORE_REQD_FOR_NOM,
NON_LAB_STD_BILL_RT_SCH_ID,
INVPROC_CURRENCY_TYPE,
REVPROC_CURRENCY_CODE,
PROJECT_BIL_RATE_DATE_CODE,
PROJECT_BIL_RATE_TYPE,
PROJECT_BIL_RATE_DATE,
PROJECT_BIL_EXCHANGE_RATE,
PROJFUNC_CURRENCY_CODE,
PROJFUNC_BIL_RATE_DATE_CODE,
PROJFUNC_BIL_RATE_TYPE,
PROJFUNC_BIL_RATE_DATE,
PROJFUNC_BIL_EXCHANGE_RATE,
FUNDING_RATE_DATE_CODE,
FUNDING_RATE_TYPE,
FUNDING_RATE_DATE,
FUNDING_EXCHANGE_RATE,
BASELINE_FUNDING_FLAG,
PROJFUNC_COST_RATE_TYPE,
PROJFUNC_COST_RATE_DATE,
INV_BY_BILL_TRANS_CURR_FLAG,
MULTI_CURRENCY_BILLING_FLAG,
SPLIT_COST_FROM_WORKPLAN_FLAG,
SPLIT_COST_FROM_BILL_FLAG,
ASSIGN_PRECEDES_TASK,
PRIORITY_CODE,
RETN_BILLING_INV_FORMAT_ID,
RETN_ACCOUNTING_FLAG,
ADV_ACTION_SET_ID,
START_ADV_ACTION_SET_FLAG,
REVALUATE_FUNDING_FLAG,
INCLUDE_GAINS_LOSSES_FLAG,
TARGET_START_DATE,
TARGET_FINISH_DATE,
BASELINE_START_DATE,
BASELINE_FINISH_DATE,
SCHEDULED_AS_OF_DATE,
BASELINE_AS_OF_DATE,
LABOR_DISC_REASON_CODE,
NON_LABOR_DISC_REASON_CODE,
SECURITY_LEVEL,
ACTUAL_AS_OF_DATE,
SCHEDULED_DURATION,
BASELINE_DURATION,
ACTUAL_DURATION,
LONG_NAME,
BTC_COST_BASE_REV_CODE,
ASSET_ALLOCATION_METHOD,
CAPITAL_EVENT_PROCESSING,
CINT_RATE_SCH_ID,
CINT_ELIGIBLE_FLAG,
CINT_STOP_DATE,
SYS_PROGRAM_FLAG,
STRUCTURE_SHARING_CODE,
ENABLE_TOP_TASK_CUSTOMER_FLAG,
ENABLE_TOP_TASK_INV_MTH_FLAG,
REVENUE_ACCRUAL_METHOD,
INVOICE_METHOD,
PROJFUNC_ATTR_FOR_AR_FLAG,
PJI_SOURCE_FLAG,
ALLOW_MULTI_PROGRAM_ROLLUP,
PROJ_REQ_RES_FORMAT_ID,
PROJ_ASGMT_RES_FORMAT_ID,
FUNDING_APPROVAL_STATUS_CODE,
REVTRANS_CURRENCY_TYPE,
DATE_EFF_FUNDS_CONSUMPTION,
AR_REC_NOTIFY_FLAG,
AUTO_RELEASE_PWP_INV,
ADJ_ON_STD_INV,
ALT_LIQUIDATION_RATE_PER,
BILL_GROUP_ID,
BILL_LABOR_ACCRUAL,
BILLING_FUNDS_CHECK_FC_FLAG,
CBS_ENABLE_FLAG,
CBS_VERSION_ID,
CONTRACT_ID,
COST_PLUS_FIXED_FEE_FLAG,
DEFERRED_ROLLUP_ENABLED_FLAG,
DERIVE_BILLING_FROM_OKE_FLAG,
IC_LABOR_TP_FIXED_DATE,
IC_LABOR_TP_SCHEDULE_ID,
IC_NL_TP_FIXED_DATE,
IC_NL_TP_SCHEDULE_ID,
FEE_PERCENTAGE,
FEE_RATE,
FEE_RATE_SCHEDULE_ID,
FEE_REVENUE_SCHEDULE_ID,
INV_LINE_GROUP,
LABOR_FEE_RATE_CURRENCY_CODE,
LAST_SUMMARIZATION_DATE,
LIQUIDATION_RATE_PERCENTAGE,
OLAP_GROUP,
OLAP_TASK_ID,
OVR_FEE_RATE_SCHEDULE_ID,
PJT_ROLLUP_ENABLED_FLAG,
POBG_FLAG,
PROGRESS_PAYMENT_FLAG,
PROJ_SOV_FRM_CNTRT_FLAG,
PROJ_SOV_LEVEL,
RATE_PERCENTAGE,
RMCS_INTEGRATION_FLAG,
SLA_ACC_END_DATE,
WITHHOLD_AMOUNT,
WP_BASELINE_VER_FLAG,
KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
 KCA_SEQ_DATE
from bec_ods_stg.pa_projects_all
where kca_operation in ('INSERT','UPDATE') and (PROJECT_ID,kca_seq_id) in (select PROJECT_ID,max(kca_seq_id) from bec_ods_stg.pa_projects_all 
where kca_operation in ('INSERT','UPDATE')
group by PROJECT_ID)

);
commit;



-- Soft delete
update bec_ods.pa_projects_all set IS_DELETED_FLG = 'N';
commit;
update bec_ods.pa_projects_all set IS_DELETED_FLG = 'Y'
where (PROJECT_ID)  in
(
select PROJECT_ID from bec_raw_dl_ext.pa_projects_all
where (PROJECT_ID,KCA_SEQ_ID)
in 
(
select PROJECT_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.pa_projects_all
group by PROJECT_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;


update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='pa_projects_all';
commit;