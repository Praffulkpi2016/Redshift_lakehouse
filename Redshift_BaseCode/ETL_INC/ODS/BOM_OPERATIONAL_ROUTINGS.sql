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
delete from 
  bec_ods.BOM_OPERATIONAL_ROUTINGS 
where 
  (
    nvl(ROUTING_SEQUENCE_ID, '0')
  ) in (
    select 
      nvl(stg.ROUTING_SEQUENCE_ID, '0') as ROUTING_SEQUENCE_ID 
    from 
      bec_ods.bom_operational_routings ods, 
      bec_ods_stg.bom_operational_routings stg 
    where 
      nvl(ods.ROUTING_SEQUENCE_ID, '0') = nvl(stg.ROUTING_SEQUENCE_ID, '0') 
      and stg.kca_operation IN ('INSERT', 'UPDATE')
  );
commit;
-- Insert records
insert into bec_ods.bom_operational_routings (
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
	IS_DELETED_FLG, 
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
    'N' AS IS_DELETED_FLG, 
    cast(NULLIF(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID, 
    kca_seq_date 
  from 
    bec_ods_stg.bom_operational_routings 
  where 
    kca_operation IN ('INSERT', 'UPDATE') 
    and (
      nvl(ROUTING_SEQUENCE_ID, '0'), 
      kca_seq_id
    ) in (
      select 
        nvl(ROUTING_SEQUENCE_ID, '0') as ROUTING_SEQUENCE_ID, 
        max(kca_seq_id) 
      from 
        bec_ods_stg.bom_operational_routings 
      where 
        kca_operation IN ('INSERT', 'UPDATE') 
      group by 
        nvl(ROUTING_SEQUENCE_ID, '0')
    )
);
commit;
-- Soft delete
update bec_ods.bom_operational_routings set IS_DELETED_FLG = 'N';
commit;
update bec_ods.bom_operational_routings set IS_DELETED_FLG = 'Y'
where (ROUTING_SEQUENCE_ID )  in
(
select ROUTING_SEQUENCE_ID  from bec_raw_dl_ext.bom_operational_routings
where (ROUTING_SEQUENCE_ID ,KCA_SEQ_ID)
in 
(
select ROUTING_SEQUENCE_ID ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.bom_operational_routings
group by ROUTING_SEQUENCE_ID 
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
  ods_table_name = 'bom_operational_routings';
commit;
