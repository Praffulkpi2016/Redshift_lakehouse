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

delete from bec_ods.gl_ledgers
where LEDGER_ID in (
select stg.LEDGER_ID from bec_ods.gl_ledgers ods, bec_ods_stg.gl_ledgers stg
where ods.LEDGER_ID = stg.LEDGER_ID  and stg.kca_operation IN ('INSERT','UPDATE') );

commit;

-- Insert records

insert into bec_ods.gl_ledgers
(
LEDGER_ID,
NAME,
SHORT_NAME,
DESCRIPTION,
LEDGER_CATEGORY_CODE,
ALC_LEDGER_TYPE_CODE,
OBJECT_TYPE_CODE,
LE_LEDGER_TYPE_CODE,
COMPLETION_STATUS_CODE,
CONFIGURATION_ID,
CHART_OF_ACCOUNTS_ID,
CURRENCY_CODE,
PERIOD_SET_NAME,
ACCOUNTED_PERIOD_TYPE,
FIRST_LEDGER_PERIOD_NAME,
RET_EARN_CODE_COMBINATION_ID,
SUSPENSE_ALLOWED_FLAG,
ALLOW_INTERCOMPANY_POST_FLAG,
TRACK_ROUNDING_IMBALANCE_FLAG,
ENABLE_AVERAGE_BALANCES_FLAG,
CUM_TRANS_CODE_COMBINATION_ID,
RES_ENCUMB_CODE_COMBINATION_ID,
NET_INCOME_CODE_COMBINATION_ID,
ROUNDING_CODE_COMBINATION_ID,
ENABLE_BUDGETARY_CONTROL_FLAG,
REQUIRE_BUDGET_JOURNALS_FLAG,
ENABLE_JE_APPROVAL_FLAG,
ENABLE_AUTOMATIC_TAX_FLAG,
CONSOLIDATION_LEDGER_FLAG,
TRANSLATE_EOD_FLAG,
TRANSLATE_QATD_FLAG,
TRANSLATE_YATD_FLAG,
TRANSACTION_CALENDAR_ID,
DAILY_TRANSLATION_RATE_TYPE,
AUTOMATICALLY_CREATED_FLAG,
BAL_SEG_VALUE_OPTION_CODE,
BAL_SEG_COLUMN_NAME,
MGT_SEG_VALUE_OPTION_CODE,
MGT_SEG_COLUMN_NAME,
BAL_SEG_VALUE_SET_ID,
MGT_SEG_VALUE_SET_ID,
IMPLICIT_ACCESS_SET_ID,
CRITERIA_SET_ID,
FUTURE_ENTERABLE_PERIODS_LIMIT,
LEDGER_ATTRIBUTES,
IMPLICIT_LEDGER_SET_ID,
LATEST_OPENED_PERIOD_NAME,
LATEST_ENCUMBRANCE_YEAR,
PERIOD_AVERAGE_RATE_TYPE,
PERIOD_END_RATE_TYPE,
BUDGET_PERIOD_AVG_RATE_TYPE,
BUDGET_PERIOD_END_RATE_TYPE,
SLA_ACCOUNTING_METHOD_CODE,
SLA_ACCOUNTING_METHOD_TYPE,
SLA_DESCRIPTION_LANGUAGE,
SLA_ENTERED_CUR_BAL_SUS_CCID,
SLA_SEQUENCING_FLAG,
SLA_BAL_BY_LEDGER_CURR_FLAG,
SLA_LEDGER_CUR_BAL_SUS_CCID,
ENABLE_SECONDARY_TRACK_FLAG,
ENABLE_REVAL_SS_TRACK_FLAG,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
CONTEXT,
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
ENABLE_RECONCILIATION_FLAG,
CREATE_JE_FLAG,
SLA_LEDGER_CASH_BASIS_FLAG,
COMPLETE_FLAG,
COMMITMENT_BUDGET_FLAG,
AUTOMATE_SEC_JRNL_REV_FLAG,
NET_CLOSING_BAL_FLAG
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID,
	kca_seq_date
)
(
select LEDGER_ID,
NAME,
SHORT_NAME,
DESCRIPTION,
LEDGER_CATEGORY_CODE,
ALC_LEDGER_TYPE_CODE,
OBJECT_TYPE_CODE,
LE_LEDGER_TYPE_CODE,
COMPLETION_STATUS_CODE,
CONFIGURATION_ID,
CHART_OF_ACCOUNTS_ID,
CURRENCY_CODE,
PERIOD_SET_NAME,
ACCOUNTED_PERIOD_TYPE,
FIRST_LEDGER_PERIOD_NAME,
RET_EARN_CODE_COMBINATION_ID,
SUSPENSE_ALLOWED_FLAG,
ALLOW_INTERCOMPANY_POST_FLAG,
TRACK_ROUNDING_IMBALANCE_FLAG,
ENABLE_AVERAGE_BALANCES_FLAG,
CUM_TRANS_CODE_COMBINATION_ID,
RES_ENCUMB_CODE_COMBINATION_ID,
NET_INCOME_CODE_COMBINATION_ID,
ROUNDING_CODE_COMBINATION_ID,
ENABLE_BUDGETARY_CONTROL_FLAG,
REQUIRE_BUDGET_JOURNALS_FLAG,
ENABLE_JE_APPROVAL_FLAG,
ENABLE_AUTOMATIC_TAX_FLAG,
CONSOLIDATION_LEDGER_FLAG,
TRANSLATE_EOD_FLAG,
TRANSLATE_QATD_FLAG,
TRANSLATE_YATD_FLAG,
TRANSACTION_CALENDAR_ID,
DAILY_TRANSLATION_RATE_TYPE,
AUTOMATICALLY_CREATED_FLAG,
BAL_SEG_VALUE_OPTION_CODE,
BAL_SEG_COLUMN_NAME,
MGT_SEG_VALUE_OPTION_CODE,
MGT_SEG_COLUMN_NAME,
BAL_SEG_VALUE_SET_ID,
MGT_SEG_VALUE_SET_ID,
IMPLICIT_ACCESS_SET_ID,
CRITERIA_SET_ID,
FUTURE_ENTERABLE_PERIODS_LIMIT,
LEDGER_ATTRIBUTES,
IMPLICIT_LEDGER_SET_ID,
LATEST_OPENED_PERIOD_NAME,
LATEST_ENCUMBRANCE_YEAR,
PERIOD_AVERAGE_RATE_TYPE,
PERIOD_END_RATE_TYPE,
BUDGET_PERIOD_AVG_RATE_TYPE,
BUDGET_PERIOD_END_RATE_TYPE,
SLA_ACCOUNTING_METHOD_CODE,
SLA_ACCOUNTING_METHOD_TYPE,
SLA_DESCRIPTION_LANGUAGE,
SLA_ENTERED_CUR_BAL_SUS_CCID,
SLA_SEQUENCING_FLAG,
SLA_BAL_BY_LEDGER_CURR_FLAG,
SLA_LEDGER_CUR_BAL_SUS_CCID,
ENABLE_SECONDARY_TRACK_FLAG,
ENABLE_REVAL_SS_TRACK_FLAG,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
CONTEXT,
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
ENABLE_RECONCILIATION_FLAG,
CREATE_JE_FLAG,
SLA_LEDGER_CASH_BASIS_FLAG,
COMPLETE_FLAG,
COMMITMENT_BUDGET_FLAG,
AUTOMATE_SEC_JRNL_REV_FLAG,
NET_CLOSING_BAL_FLAG
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from bec_ods_stg.gl_ledgers
where kca_operation IN ('INSERT','UPDATE') and (LEDGER_ID,kca_seq_id) in (select LEDGER_ID,max(kca_seq_id) from bec_ods_stg.gl_ledgers 
where kca_operation IN ('INSERT','UPDATE')
group by LEDGER_ID)
);

commit;

 
-- Soft Delete 
update bec_ods.gl_ledgers set IS_DELETED_FLG = 'N';
commit;
update bec_ods.gl_ledgers set IS_DELETED_FLG = 'Y'
where (LEDGER_ID)  in
(
select LEDGER_ID from bec_raw_dl_ext.gl_ledgers
where (LEDGER_ID,KCA_SEQ_ID)
in 
(
select LEDGER_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.gl_ledgers
group by LEDGER_ID
) 
and kca_operation= 'DELETE'
);
commit;
end; 
update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='gl_ledgers';
commit;

