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

delete
from
	bec_ods.BOM_OPERATION_RESOURCES
where
	(
	nvl(OPERATION_SEQUENCE_ID, 0) ,nvl(RESOURCE_SEQ_NUM, 0) 
	) in 
	(
	select
		NVL(stg.OPERATION_SEQUENCE_ID, 0) as OPERATION_SEQUENCE_ID ,
		NVL(stg.RESOURCE_SEQ_NUM, 0) as RESOURCE_SEQ_NUM 
	from
		bec_ods.bom_operation_resources ods,
		bec_ods_stg.bom_operation_resources stg
	where
		    NVL(ods.OPERATION_SEQUENCE_ID, 0) = NVL(stg.OPERATION_SEQUENCE_ID, 0) AND 
			NVL(ods.RESOURCE_SEQ_NUM, 0) = NVL(stg.RESOURCE_SEQ_NUM, 0)
					and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.bom_operation_resources (
	OPERATION_SEQUENCE_ID, 
	RESOURCE_SEQ_NUM, 
	RESOURCE_ID, 
	ACTIVITY_ID, 
	STANDARD_RATE_FLAG, 
	ASSIGNED_UNITS, 
	USAGE_RATE_OR_AMOUNT, 
	USAGE_RATE_OR_AMOUNT_INVERSE, 
	BASIS_TYPE, 
	SCHEDULE_FLAG, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_LOGIN, 
	RESOURCE_OFFSET_PERCENT, 
	AUTOCHARGE_TYPE, 
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
	REQUEST_ID, 
	PROGRAM_APPLICATION_ID, 
	PROGRAM_ID, 
	PROGRAM_UPDATE_DATE, 
	SCHEDULE_SEQ_NUM, 
	SUBSTITUTE_GROUP_NUM, 
	PRINCIPLE_FLAG, 
	SETUP_ID, 
	CHANGE_NOTICE, 
	ACD_TYPE, 
	ORIGINAL_SYSTEM_REFERENCE,	
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
		OPERATION_SEQUENCE_ID, 
		RESOURCE_SEQ_NUM, 
		RESOURCE_ID, 
		ACTIVITY_ID, 
		STANDARD_RATE_FLAG, 
		ASSIGNED_UNITS, 
		USAGE_RATE_OR_AMOUNT, 
		USAGE_RATE_OR_AMOUNT_INVERSE, 
		BASIS_TYPE, 
		SCHEDULE_FLAG, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		RESOURCE_OFFSET_PERCENT, 
		AUTOCHARGE_TYPE, 
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
		REQUEST_ID, 
		PROGRAM_APPLICATION_ID, 
		PROGRAM_ID, 
		PROGRAM_UPDATE_DATE, 
		SCHEDULE_SEQ_NUM, 
		SUBSTITUTE_GROUP_NUM, 
		PRINCIPLE_FLAG, 
		SETUP_ID, 
		CHANGE_NOTICE, 
		ACD_TYPE, 
		ORIGINAL_SYSTEM_REFERENCE,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.bom_operation_resources
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		nvl(OPERATION_SEQUENCE_ID, 0) , nvl(RESOURCE_SEQ_NUM, 0),
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(OPERATION_SEQUENCE_ID, 0) as OPERATION_SEQUENCE_ID ,
			nvl(RESOURCE_SEQ_NUM, 0) as RESOURCE_SEQ_NUM ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.bom_operation_resources
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			OPERATION_SEQUENCE_ID ,RESOURCE_SEQ_NUM
			)	
	);

commit;
-- Soft delete
update bec_ods.bom_operation_resources set IS_DELETED_FLG = 'N';
commit;
update bec_ods.bom_operation_resources set IS_DELETED_FLG = 'Y'
where (nvl(OPERATION_SEQUENCE_ID, 0),nvl(RESOURCE_SEQ_NUM, 0) )  in
(
select nvl(OPERATION_SEQUENCE_ID, 0),nvl(RESOURCE_SEQ_NUM, 0)  from bec_raw_dl_ext.bom_operation_resources
where (nvl(OPERATION_SEQUENCE_ID, 0),nvl(RESOURCE_SEQ_NUM, 0) ,KCA_SEQ_ID)
in 
(
select nvl(OPERATION_SEQUENCE_ID, 0),nvl(RESOURCE_SEQ_NUM, 0) ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.bom_operation_resources
group by nvl(OPERATION_SEQUENCE_ID, 0),nvl(RESOURCE_SEQ_NUM, 0) 
) 
and kca_operation= 'DELETE'
);
commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'bom_operation_resources';

commit;