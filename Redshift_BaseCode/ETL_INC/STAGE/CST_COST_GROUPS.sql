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

truncate table bec_ods_stg.CST_COST_GROUPS;

insert into	bec_ods_stg.CST_COST_GROUPS
   (COST_GROUP_ID,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	REQUEST_ID,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	PROGRAM_UPDATE_DATE,
	ORGANIZATION_ID,
	COST_GROUP,
	DESCRIPTION,
	DISABLE_DATE,
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
	LEGAL_ENTITY,
	COST_GROUP_TYPE,
	ZD_EDITION_NAME,
	ZD_SYNC,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		COST_GROUP_ID,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
		ORGANIZATION_ID,
		COST_GROUP,
		DESCRIPTION,
		DISABLE_DATE,
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
		LEGAL_ENTITY,
		COST_GROUP_TYPE,
		ZD_EDITION_NAME,
		ZD_SYNC,
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.CST_COST_GROUPS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (COST_GROUP_ID,kca_seq_id) in 
	(select COST_GROUP_ID,max(kca_seq_id) from bec_raw_dl_ext.CST_COST_GROUPS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by COST_GROUP_ID)
        and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'cst_cost_groups')
);
end;