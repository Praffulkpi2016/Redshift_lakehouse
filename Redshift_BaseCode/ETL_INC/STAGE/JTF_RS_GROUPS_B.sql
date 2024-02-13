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

truncate
	table bec_ods_stg.JTF_RS_GROUPS_B;

COMMIT;

insert
	into
	bec_ods_stg.JTF_RS_GROUPS_B
(	
	GROUP_ID,  
	GROUP_NUMBER,  
	CREATED_BY,  
	CREATION_DATE, 
	LAST_UPDATED_BY,  
	LAST_UPDATE_DATE, 
	LAST_UPDATE_LOGIN,  
	EXCLUSIVE_FLAG,  
	START_DATE_ACTIVE, 
	END_DATE_ACTIVE, 
	ACCOUNTING_CODE,  
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
	EMAIL_ADDRESS,  
	OBJECT_VERSION_NUMBER,  
	SECURITY_GROUP_ID,  
	TIME_ZONE,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		GROUP_ID,  
		GROUP_NUMBER,  
		CREATED_BY,  
		CREATION_DATE, 
		LAST_UPDATED_BY,  
		LAST_UPDATE_DATE, 
		LAST_UPDATE_LOGIN,  
		EXCLUSIVE_FLAG,  
		START_DATE_ACTIVE, 
		END_DATE_ACTIVE, 
		ACCOUNTING_CODE,  
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
		EMAIL_ADDRESS,  
		OBJECT_VERSION_NUMBER,  
		SECURITY_GROUP_ID,  
		TIME_ZONE,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.JTF_RS_GROUPS_B
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(GROUP_ID, 0),
		kca_seq_id) in 
(
		select
			nvl(GROUP_ID,0) as GROUP_ID,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.JTF_RS_GROUPS_B
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(GROUP_ID, 0))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'jtf_rs_groups_b')
);
end;