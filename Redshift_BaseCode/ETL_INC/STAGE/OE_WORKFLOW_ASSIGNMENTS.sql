/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods_stg.oe_workflow_assignments;

insert into	bec_ods_stg.oe_workflow_assignments
   (	
	ORDER_TYPE_ID,
	LINE_TYPE_ID,
	PROCESS_NAME,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	REQUEST_ID,
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
	ITEM_TYPE_CODE,
	ASSIGNMENT_ID,
	START_DATE_ACTIVE,
	END_DATE_ACTIVE,
	WF_ITEM_TYPE,
    KCA_OPERATION,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
		ORDER_TYPE_ID,
		LINE_TYPE_ID,
		PROCESS_NAME,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		REQUEST_ID,
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
		ITEM_TYPE_CODE,
		ASSIGNMENT_ID,
		START_DATE_ACTIVE,
		END_DATE_ACTIVE,
		WF_ITEM_TYPE, 
        KCA_OPERATION,
		kca_seq_id
		,KCA_SEQ_DATE
	from bec_raw_dl_ext.oe_workflow_assignments
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (nvl(ASSIGNMENT_ID,0),kca_seq_id) in 
	(select nvl(ASSIGNMENT_ID,0) as ASSIGNMENT_ID,max(kca_seq_id) from bec_raw_dl_ext.oe_workflow_assignments 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by nvl(ASSIGNMENT_ID,0))
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'oe_workflow_assignments')
)
);
end;