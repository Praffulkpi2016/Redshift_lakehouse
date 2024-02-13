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

delete from bec_ods.BOM_STANDARD_OPERATIONS
where (nvl(LINE_ID,0),nvl(OPERATION_TYPE,0),nvl(STANDARD_OPERATION_ID, 0)) in (
select nvl(stg.LINE_ID,0) as LINE_ID,
nvl(stg.OPERATION_TYPE,0) as OPERATION_TYPE,
nvl(stg.STANDARD_OPERATION_ID, 0) as STANDARD_OPERATION_ID
from bec_ods.bom_standard_operations ods, bec_ods_stg.bom_standard_operations stg
where nvl(ods.LINE_ID,0) = nvl(stg.LINE_ID,0) 
and nvl(ods.OPERATION_TYPE,0) = nvl(stg.OPERATION_TYPE,0)
and nvl(ods.STANDARD_OPERATION_ID, 0) = nvl(stg.STANDARD_OPERATION_ID, 0)
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.bom_standard_operations
       (	
		SEQUENCE_NUM, 
		LINE_ID, 
		OPERATION_TYPE, 
		STANDARD_OPERATION_ID, 
		OPERATION_CODE, 
		ORGANIZATION_ID, 
		DEPARTMENT_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		MINIMUM_TRANSFER_QUANTITY, 
		COUNT_POINT_TYPE, 
		OPERATION_DESCRIPTION, 
		OPTION_DEPENDENT_FLAG, 
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
		BACKFLUSH_FLAG, 
		WMS_TASK_TYPE, 
		INCLUDE_IN_ROLLUP, 
		YIELD, 
		OPERATION_YIELD_ENABLED, 
		SHUTDOWN_TYPE, 
		ACTUAL_IPK, 
		CRITICAL_TO_QUALITY, 
		VALUE_ADDED, 
		DEFAULT_SUBINVENTORY, 
		DEFAULT_LOCATOR_ID, 
		LOWEST_ACCEPTABLE_YIELD, 
		USE_ORG_SETTINGS, 
		QUEUE_MANDATORY_FLAG, 
		RUN_MANDATORY_FLAG, 
		TO_MOVE_MANDATORY_FLAG, 
		SHOW_NEXT_OP_BY_DEFAULT, 
		SHOW_SCRAP_CODE, 
		SHOW_LOT_ATTRIB, 
		TRACK_MULTIPLE_RES_USAGE_DATES, 
		CHECK_SKILL, 
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_ID,
		kca_seq_date)	
(
	select
		SEQUENCE_NUM, 
		LINE_ID, 
		OPERATION_TYPE, 
		STANDARD_OPERATION_ID, 
		OPERATION_CODE, 
		ORGANIZATION_ID, 
		DEPARTMENT_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		MINIMUM_TRANSFER_QUANTITY, 
		COUNT_POINT_TYPE, 
		OPERATION_DESCRIPTION, 
		OPTION_DEPENDENT_FLAG, 
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
		BACKFLUSH_FLAG, 
		WMS_TASK_TYPE, 
		INCLUDE_IN_ROLLUP, 
		YIELD, 
		OPERATION_YIELD_ENABLED, 
		SHUTDOWN_TYPE, 
		ACTUAL_IPK, 
		CRITICAL_TO_QUALITY, 
		VALUE_ADDED, 
		DEFAULT_SUBINVENTORY, 
		DEFAULT_LOCATOR_ID, 
		LOWEST_ACCEPTABLE_YIELD, 
		USE_ORG_SETTINGS, 
		QUEUE_MANDATORY_FLAG, 
		RUN_MANDATORY_FLAG, 
		TO_MOVE_MANDATORY_FLAG, 
		SHOW_NEXT_OP_BY_DEFAULT, 
		SHOW_SCRAP_CODE, 
		SHOW_LOT_ATTRIB, 
		TRACK_MULTIPLE_RES_USAGE_DATES, 
		CHECK_SKILL, 
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.bom_standard_operations
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(LINE_ID,0),nvl(OPERATION_TYPE,0),nvl(STANDARD_OPERATION_ID, 0),kca_seq_ID) in 
	(select nvl(LINE_ID,0) as LINE_ID,nvl(OPERATION_TYPE,0) as OPERATION_TYPE,nvl(STANDARD_OPERATION_ID, 0) as STANDARD_OPERATION_ID,max(kca_seq_ID) as kca_seq_ID from bec_ods_stg.bom_standard_operations 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(LINE_ID,0),nvl(OPERATION_TYPE,0),nvl(STANDARD_OPERATION_ID, 0))
);

commit;

-- Soft delete
update bec_ods.bom_standard_operations set IS_DELETED_FLG = 'N';
commit;
update bec_ods.bom_standard_operations set IS_DELETED_FLG = 'Y'
where (nvl(LINE_ID,0),nvl(OPERATION_TYPE,0),nvl(STANDARD_OPERATION_ID, 0) )  in
(
select nvl(LINE_ID,0),nvl(OPERATION_TYPE,0),nvl(STANDARD_OPERATION_ID, 0)  from bec_raw_dl_ext.bom_standard_operations
where (nvl(LINE_ID,0),nvl(OPERATION_TYPE,0),nvl(STANDARD_OPERATION_ID, 0) ,KCA_SEQ_ID)
in 
(
select nvl(LINE_ID,0),nvl(OPERATION_TYPE,0),nvl(STANDARD_OPERATION_ID, 0) ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.bom_standard_operations
group by nvl(LINE_ID,0),nvl(OPERATION_TYPE,0),nvl(STANDARD_OPERATION_ID, 0) 
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'bom_standard_operations';

commit;