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
drop table if exists bec_ods.GL_JE_BATCHES;

CREATE TABLE IF NOT EXISTS bec_ods.GL_JE_BATCHES
(
JE_BATCH_ID	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,SET_OF_BOOKS_ID_11I	NUMERIC(15,0)   ENCODE az64
,NAME	VARCHAR(100)   ENCODE lzo
,STATUS	VARCHAR(1)   ENCODE lzo
,STATUS_VERIFIED	VARCHAR(1)   ENCODE lzo
,ACTUAL_FLAG	VARCHAR(1)   ENCODE lzo
,DEFAULT_EFFECTIVE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,BUDGETARY_CONTROL_STATUS	VARCHAR(1)   ENCODE lzo
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,STATUS_RESET_FLAG	VARCHAR(1)   ENCODE lzo
,DEFAULT_PERIOD_NAME	VARCHAR(15)   ENCODE lzo
,UNIQUE_DATE	VARCHAR(30)   ENCODE lzo
,EARLIEST_POSTABLE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,POSTED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,DATE_CREATED TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,DESCRIPTION	VARCHAR(240)   ENCODE lzo
,CONTROL_TOTAL	NUMERIC(28,10)   ENCODE az64
,RUNNING_TOTAL_DR	NUMERIC(28,10)   ENCODE az64
,RUNNING_TOTAL_CR	NUMERIC(28,10)   ENCODE az64
,RUNNING_TOTAL_ACCOUNTED_DR	NUMERIC(28,10)   ENCODE az64
,RUNNING_TOTAL_ACCOUNTED_CR	NUMERIC(28,10)   ENCODE az64
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
,CONTEXT	VARCHAR(150)   ENCODE lzo
,PACKET_ID	NUMERIC(15,0)   ENCODE az64
,USSGL_TRANSACTION_CODE	VARCHAR(30)   ENCODE lzo
,CONTEXT2	VARCHAR(150)   ENCODE lzo
,POSTING_RUN_ID	NUMERIC(15,0)   ENCODE az64
,REQUEST_ID	NUMERIC(15,0)   ENCODE az64
,UNRESERVATION_PACKET_ID	NUMERIC(15,0)   ENCODE az64
,AVERAGE_JOURNAL_FLAG	VARCHAR(1)   ENCODE lzo
,ORG_ID	NUMERIC(15,0)   ENCODE az64
,APPROVAL_STATUS_CODE	VARCHAR(1)   ENCODE lzo
,PARENT_JE_BATCH_ID	NUMERIC(15,0)   ENCODE az64
,POSTED_BY	NUMERIC(15,0)   ENCODE az64
,CHART_OF_ACCOUNTS_ID	NUMERIC(15,0)   ENCODE az64
,PERIOD_SET_NAME	VARCHAR(15)   ENCODE lzo
,ACCOUNTED_PERIOD_TYPE	VARCHAR(15)   ENCODE lzo
,GROUP_ID	NUMERIC(15,0)   ENCODE az64
,APPROVER_EMPLOYEE_ID	NUMERIC(15,0)   ENCODE az64
,GLOBAL_ATTRIBUTE_CATEGORY	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE9	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE10	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE11	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE12	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE13	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE14	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE15	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE16	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE17	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE18	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE19	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE20	VARCHAR(150)   ENCODE lzo
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64  ) 
DISTSTYLE AUTO
;

insert into bec_ods.GL_JE_BATCHES
(
JE_BATCH_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,SET_OF_BOOKS_ID_11I
,NAME
,STATUS
,STATUS_VERIFIED
,ACTUAL_FLAG
,DEFAULT_EFFECTIVE_DATE
,BUDGETARY_CONTROL_STATUS
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,STATUS_RESET_FLAG
,DEFAULT_PERIOD_NAME
,UNIQUE_DATE
,EARLIEST_POSTABLE_DATE
,POSTED_DATE
,DATE_CREATED
,DESCRIPTION
,CONTROL_TOTAL
,RUNNING_TOTAL_DR
,RUNNING_TOTAL_CR
,RUNNING_TOTAL_ACCOUNTED_DR
,RUNNING_TOTAL_ACCOUNTED_CR
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
,CONTEXT
,PACKET_ID
,USSGL_TRANSACTION_CODE
,CONTEXT2
,POSTING_RUN_ID
,REQUEST_ID
,UNRESERVATION_PACKET_ID
,AVERAGE_JOURNAL_FLAG
,ORG_ID
,APPROVAL_STATUS_CODE
,PARENT_JE_BATCH_ID
,POSTED_BY
,CHART_OF_ACCOUNTS_ID
,PERIOD_SET_NAME
,ACCOUNTED_PERIOD_TYPE
,GROUP_ID
,APPROVER_EMPLOYEE_ID
,GLOBAL_ATTRIBUTE_CATEGORY
,GLOBAL_ATTRIBUTE1
,GLOBAL_ATTRIBUTE2
,GLOBAL_ATTRIBUTE3
,GLOBAL_ATTRIBUTE4
,GLOBAL_ATTRIBUTE5
,GLOBAL_ATTRIBUTE6
,GLOBAL_ATTRIBUTE7
,GLOBAL_ATTRIBUTE8
,GLOBAL_ATTRIBUTE9
,GLOBAL_ATTRIBUTE10
,GLOBAL_ATTRIBUTE11
,GLOBAL_ATTRIBUTE12
,GLOBAL_ATTRIBUTE13
,GLOBAL_ATTRIBUTE14
,GLOBAL_ATTRIBUTE15
,GLOBAL_ATTRIBUTE16
,GLOBAL_ATTRIBUTE17
,GLOBAL_ATTRIBUTE18
,GLOBAL_ATTRIBUTE19
,GLOBAL_ATTRIBUTE20
,KCA_OPERATION
,IS_DELETED_FLG
,kca_seq_id,
	kca_seq_date
)
(
select
JE_BATCH_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,SET_OF_BOOKS_ID_11I
,NAME
,STATUS
,STATUS_VERIFIED
,ACTUAL_FLAG
,DEFAULT_EFFECTIVE_DATE
,BUDGETARY_CONTROL_STATUS
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,STATUS_RESET_FLAG
,DEFAULT_PERIOD_NAME
,UNIQUE_DATE
,EARLIEST_POSTABLE_DATE
,POSTED_DATE
,DATE_CREATED
,DESCRIPTION
,CONTROL_TOTAL
,RUNNING_TOTAL_DR
,RUNNING_TOTAL_CR
,RUNNING_TOTAL_ACCOUNTED_DR
,RUNNING_TOTAL_ACCOUNTED_CR
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
,CONTEXT
,PACKET_ID
,USSGL_TRANSACTION_CODE
,CONTEXT2
,POSTING_RUN_ID
,REQUEST_ID
,UNRESERVATION_PACKET_ID
,AVERAGE_JOURNAL_FLAG
,ORG_ID
,APPROVAL_STATUS_CODE
,PARENT_JE_BATCH_ID
,POSTED_BY
,CHART_OF_ACCOUNTS_ID
,PERIOD_SET_NAME
,ACCOUNTED_PERIOD_TYPE
,GROUP_ID
,APPROVER_EMPLOYEE_ID
,GLOBAL_ATTRIBUTE_CATEGORY
,GLOBAL_ATTRIBUTE1
,GLOBAL_ATTRIBUTE2
,GLOBAL_ATTRIBUTE3
,GLOBAL_ATTRIBUTE4
,GLOBAL_ATTRIBUTE5
,GLOBAL_ATTRIBUTE6
,GLOBAL_ATTRIBUTE7
,GLOBAL_ATTRIBUTE8
,GLOBAL_ATTRIBUTE9
,GLOBAL_ATTRIBUTE10
,GLOBAL_ATTRIBUTE11
,GLOBAL_ATTRIBUTE12
,GLOBAL_ATTRIBUTE13
,GLOBAL_ATTRIBUTE14
,GLOBAL_ATTRIBUTE15
,GLOBAL_ATTRIBUTE16
,GLOBAL_ATTRIBUTE17
,GLOBAL_ATTRIBUTE18
,GLOBAL_ATTRIBUTE19
,GLOBAL_ATTRIBUTE20
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
from  bec_ods_stg.GL_JE_BATCHES
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='gl_je_batches'; 

commit;