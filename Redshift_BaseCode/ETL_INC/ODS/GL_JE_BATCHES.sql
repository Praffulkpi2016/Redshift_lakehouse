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

delete from bec_ods.gl_je_batches
where je_batch_ID in (
select stg.je_batch_ID from bec_ods.gl_je_batches ods, bec_ods_stg.gl_je_batches stg
where ods.je_batch_ID = stg.je_batch_ID  and stg.kca_operation IN ('INSERT','UPDATE') );

commit;
 
 -- Insert records

insert into bec_ods.gl_je_batches
(
JE_BATCH_ID,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
SET_OF_BOOKS_ID_11I,
NAME,
STATUS,
STATUS_VERIFIED,
ACTUAL_FLAG,
DEFAULT_EFFECTIVE_DATE,
BUDGETARY_CONTROL_STATUS,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
STATUS_RESET_FLAG,
DEFAULT_PERIOD_NAME,
UNIQUE_DATE,
EARLIEST_POSTABLE_DATE,
POSTED_DATE,
DATE_CREATED,
DESCRIPTION,
CONTROL_TOTAL,
RUNNING_TOTAL_DR,
RUNNING_TOTAL_CR,
RUNNING_TOTAL_ACCOUNTED_DR,
RUNNING_TOTAL_ACCOUNTED_CR,
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
CONTEXT,
PACKET_ID,
USSGL_TRANSACTION_CODE,
CONTEXT2,
POSTING_RUN_ID,
REQUEST_ID,
UNRESERVATION_PACKET_ID,
AVERAGE_JOURNAL_FLAG,
ORG_ID,
APPROVAL_STATUS_CODE,
PARENT_JE_BATCH_ID,
POSTED_BY,
CHART_OF_ACCOUNTS_ID,
PERIOD_SET_NAME,
ACCOUNTED_PERIOD_TYPE,
GROUP_ID,
APPROVER_EMPLOYEE_ID,
GLOBAL_ATTRIBUTE_CATEGORY,
GLOBAL_ATTRIBUTE1,
GLOBAL_ATTRIBUTE2,
GLOBAL_ATTRIBUTE3,
GLOBAL_ATTRIBUTE4,
GLOBAL_ATTRIBUTE5,
GLOBAL_ATTRIBUTE6,
GLOBAL_ATTRIBUTE7,
GLOBAL_ATTRIBUTE8,
GLOBAL_ATTRIBUTE9,
GLOBAL_ATTRIBUTE10,
GLOBAL_ATTRIBUTE11,
GLOBAL_ATTRIBUTE12,
GLOBAL_ATTRIBUTE13,
GLOBAL_ATTRIBUTE14,
GLOBAL_ATTRIBUTE15,
GLOBAL_ATTRIBUTE16,
GLOBAL_ATTRIBUTE17,
GLOBAL_ATTRIBUTE18,
GLOBAL_ATTRIBUTE19,
GLOBAL_ATTRIBUTE20
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID,
	kca_seq_date
)
(
select JE_BATCH_ID,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
SET_OF_BOOKS_ID_11I,
NAME,
STATUS,
STATUS_VERIFIED,
ACTUAL_FLAG,
DEFAULT_EFFECTIVE_DATE,
BUDGETARY_CONTROL_STATUS,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
STATUS_RESET_FLAG,
DEFAULT_PERIOD_NAME,
UNIQUE_DATE,
EARLIEST_POSTABLE_DATE,
POSTED_DATE,
DATE_CREATED,
DESCRIPTION,
CONTROL_TOTAL,
RUNNING_TOTAL_DR,
RUNNING_TOTAL_CR,
RUNNING_TOTAL_ACCOUNTED_DR,
RUNNING_TOTAL_ACCOUNTED_CR,
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
CONTEXT,
PACKET_ID,
USSGL_TRANSACTION_CODE,
CONTEXT2,
POSTING_RUN_ID,
REQUEST_ID,
UNRESERVATION_PACKET_ID,
AVERAGE_JOURNAL_FLAG,
ORG_ID,
APPROVAL_STATUS_CODE,
PARENT_JE_BATCH_ID,
POSTED_BY,
CHART_OF_ACCOUNTS_ID,
PERIOD_SET_NAME,
ACCOUNTED_PERIOD_TYPE,
GROUP_ID,
APPROVER_EMPLOYEE_ID,
GLOBAL_ATTRIBUTE_CATEGORY,
GLOBAL_ATTRIBUTE1,
GLOBAL_ATTRIBUTE2,
GLOBAL_ATTRIBUTE3,
GLOBAL_ATTRIBUTE4,
GLOBAL_ATTRIBUTE5,
GLOBAL_ATTRIBUTE6,
GLOBAL_ATTRIBUTE7,
GLOBAL_ATTRIBUTE8,
GLOBAL_ATTRIBUTE9,
GLOBAL_ATTRIBUTE10,
GLOBAL_ATTRIBUTE11,
GLOBAL_ATTRIBUTE12,
GLOBAL_ATTRIBUTE13,
GLOBAL_ATTRIBUTE14,
GLOBAL_ATTRIBUTE15,
GLOBAL_ATTRIBUTE16,
GLOBAL_ATTRIBUTE17,
GLOBAL_ATTRIBUTE18,
GLOBAL_ATTRIBUTE19,
GLOBAL_ATTRIBUTE20
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from bec_ods_stg.gl_je_batches
where kca_operation IN ('INSERT','UPDATE') and (je_batch_ID,kca_seq_id) in (select je_batch_ID,max(kca_seq_id) from bec_ods_stg.gl_je_batches 
where kca_operation IN ('INSERT','UPDATE')
group by je_batch_ID)
);

commit; 
 
-- Soft delete 
update bec_ods.gl_je_batches set IS_DELETED_FLG = 'N';
commit;
update bec_ods.gl_je_batches set IS_DELETED_FLG = 'Y'
where (je_batch_ID)  in
(
select je_batch_ID from bec_raw_dl_ext.gl_je_batches
where (je_batch_ID,KCA_SEQ_ID)
in 
(
select je_batch_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.gl_je_batches
group by je_batch_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;
 
update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='gl_je_batches';
commit;

