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

truncate table bec_ods_stg.BOM_OPERATIONAL_ROUTINGS;

insert into bec_ods_stg.bom_operational_routings (
  ROUTING_SEQUENCE_ID, 
  ASSEMBLY_ITEM_ID, 
  ORGANIZATION_ID, 
  ALTERNATE_ROUTING_DESIGNATOR, 
  LAST_UPDATE_DATE, 
  LAST_UPDATED_BY, 
  CREATION_DATE, 
  CREATED_BY, 
  LAST_UPDATE_LOGIN, 
  ROUTING_TYPE, 
  COMMON_ASSEMBLY_ITEM_ID, 
  COMMON_ROUTING_SEQUENCE_ID, 
  ROUTING_COMMENT, 
  COMPLETION_SUBINVENTORY, 
  COMPLETION_LOCATOR_ID, 
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
  LINE_ID, 
  CFM_ROUTING_FLAG, 
  MIXED_MODEL_MAP_FLAG, 
  PRIORITY, 
  TOTAL_PRODUCT_CYCLE_TIME, 
  CTP_FLAG, 
  PROJECT_ID, 
  TASK_ID, 
  PENDING_FROM_ECN, 
  ORIGINAL_SYSTEM_REFERENCE, 
  SERIALIZATION_START_OP, 
  KCA_OPERATION, 
  kca_seq_id, 
  kca_seq_date
) (
  select
    ROUTING_SEQUENCE_ID, 
	ASSEMBLY_ITEM_ID, 
	ORGANIZATION_ID, 
	ALTERNATE_ROUTING_DESIGNATOR, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_LOGIN, 
	ROUTING_TYPE, 
	COMMON_ASSEMBLY_ITEM_ID, 
	COMMON_ROUTING_SEQUENCE_ID, 
	ROUTING_COMMENT, 
	COMPLETION_SUBINVENTORY, 
	COMPLETION_LOCATOR_ID, 
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
	LINE_ID, 
	CFM_ROUTING_FLAG, 
	MIXED_MODEL_MAP_FLAG, 
	PRIORITY, 
	TOTAL_PRODUCT_CYCLE_TIME, 
	CTP_FLAG, 
	PROJECT_ID, 
	TASK_ID, 
	PENDING_FROM_ECN, 
	ORIGINAL_SYSTEM_REFERENCE, 
	SERIALIZATION_START_OP,   
    KCA_OPERATION, 
    kca_seq_id, 
    kca_seq_date 
  from 
    bec_raw_dl_ext.bom_operational_routings 
  where 
    kca_operation != 'DELETE' 
    and nvl(kca_seq_id, '')!= '' 
    and (
      nvl(ROUTING_SEQUENCE_ID, '0'), 
      kca_seq_id
    ) in (
      select 
        nvl(ROUTING_SEQUENCE_ID, '0') as ROUTING_SEQUENCE_ID, 
        max(kca_seq_id) 
      from 
        bec_raw_dl_ext.bom_operational_routings 
      where 
        kca_operation != 'DELETE' 
        and nvl(kca_seq_id, '')!= '' 
      group by 
        nvl(ROUTING_SEQUENCE_ID, '0')
    ) 
    and kca_seq_date > (
      select 
        (executebegints - prune_days) 
      from 
        bec_etl_ctrl.batch_ods_info 
      where 
        ods_table_name = 'bom_operational_routings'
    )
);
end;
