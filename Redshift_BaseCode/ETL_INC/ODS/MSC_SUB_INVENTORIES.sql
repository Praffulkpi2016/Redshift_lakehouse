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
  bec_ods.MSC_SUB_INVENTORIES 
where 
  (
    nvl(PLAN_ID, 0), 
    nvl(SR_INSTANCE_ID, 0), 
    nvl(ORGANIZATION_ID, 0), 
    nvl(SUB_INVENTORY_CODE, 'NA')
  ) in (
    select 
      nvl(stg.PLAN_ID, 0) as PLAN_ID, 
      nvl(stg.SR_INSTANCE_ID, 0) as SR_INSTANCE_ID, 
      nvl(stg.ORGANIZATION_ID, 0) as ORGANIZATION_ID, 
      nvl(stg.SUB_INVENTORY_CODE, 'NA') as SUB_INVENTORY_CODE 
    from 
      bec_ods.MSC_SUB_INVENTORIES ods, 
      bec_ods_stg.MSC_SUB_INVENTORIES stg 
    where 
      nvl(ods.PLAN_ID, 0) = nvl(stg.PLAN_ID, 0) 
      and nvl(ods.SR_INSTANCE_ID, 0) = nvl(stg.SR_INSTANCE_ID, 0) 
      and nvl(ods.ORGANIZATION_ID, 0) = nvl(stg.ORGANIZATION_ID, 0) 
      and nvl(ods.SUB_INVENTORY_CODE, 'NA') = nvl(stg.SUB_INVENTORY_CODE, 'NA') 
      and stg.kca_operation IN ('INSERT', 'UPDATE')
  );


commit;


-- Insert records

insert into	bec_ods.MSC_SUB_INVENTORIES
(
	PLAN_ID
	,ORGANIZATION_ID
	,SUB_INVENTORY_CODE
	,NETTING_TYPE
	,SR_INSTANCE_ID
	,DESCRIPTION
	,REFRESH_NUMBER
	,INVENTORY_ATP_CODE
	,LAST_UPDATE_DATE
	,LAST_UPDATED_BY
	,CREATION_DATE
	,CREATED_BY
	,LAST_UPDATE_LOGIN
	,REQUEST_ID
	,PROGRAM_APPLICATION_ID
	,PROGRAM_ID
	,PROGRAM_UPDATE_DATE
	,ATTRIBUTE_CATEGORY
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
	,ATTRIBUTE11
	,ATTRIBUTE12
	,ATTRIBUTE13
	,ATTRIBUTE14
	,ATTRIBUTE15
	,INCLUDE_NON_NETTABLE
	,SR_RESOURCE_NAME
	,SR_CUSTOMER_ACCT_ID
	,CONDITION_TYPE
	,KCA_OPERATION
	,IS_DELETED_FLG
	,kca_seq_id
	,kca_seq_date)
(select
	PLAN_ID
	,ORGANIZATION_ID
	,SUB_INVENTORY_CODE
	,NETTING_TYPE
	,SR_INSTANCE_ID
	,DESCRIPTION
	,REFRESH_NUMBER
	,INVENTORY_ATP_CODE
	,LAST_UPDATE_DATE
	,LAST_UPDATED_BY
	,CREATION_DATE
	,CREATED_BY
	,LAST_UPDATE_LOGIN
	,REQUEST_ID
	,PROGRAM_APPLICATION_ID
	,PROGRAM_ID
	,PROGRAM_UPDATE_DATE
	,ATTRIBUTE_CATEGORY
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
	,ATTRIBUTE11
	,ATTRIBUTE12
	,ATTRIBUTE13
	,ATTRIBUTE14
	,ATTRIBUTE15
	,INCLUDE_NON_NETTABLE
	,SR_RESOURCE_NAME
	,SR_CUSTOMER_ACCT_ID
	,CONDITION_TYPE
	,KCA_OPERATION
	,'N' AS IS_DELETED_FLG
	,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
from 
  bec_ods_stg.MSC_SUB_INVENTORIES 
where 
  kca_operation IN ('INSERT', 'UPDATE') 
  and (
    nvl(PLAN_ID, 0), 
    nvl(SR_INSTANCE_ID, 0), 
    nvl(ORGANIZATION_ID, 0), 
    nvl(SUB_INVENTORY_CODE, 'NA'), 
    KCA_SEQ_ID
  ) in (
    select 
      nvl(PLAN_ID, 0) as PLAN_ID, 
      nvl(SR_INSTANCE_ID, 0) as SR_INSTANCE_ID, 
      nvl(ORGANIZATION_ID, 0) as ORGANIZATION_ID, 
      nvl(SUB_INVENTORY_CODE, 'NA') as SUB_INVENTORY_CODE, 
      max(KCA_SEQ_ID) 
    from 
      bec_ods_stg.MSC_SUB_INVENTORIES 
    where 
      kca_operation IN ('INSERT', 'UPDATE') 
    group by 
      nvl(PLAN_ID, 0), 
      nvl(SR_INSTANCE_ID, 0), 
      nvl(ORGANIZATION_ID, 0), 
      nvl(SUB_INVENTORY_CODE, 'NA')
  )
);


commit;

-- Soft delete
update bec_ods.MSC_SUB_INVENTORIES set IS_DELETED_FLG = 'N';

commit;

update 
  bec_ods.MSC_SUB_INVENTORIES 
set 
  IS_DELETED_FLG = 'Y' 
where 
  (
    nvl(PLAN_ID, 0), 
    nvl(SR_INSTANCE_ID, 0), 
    nvl(ORGANIZATION_ID, 0), 
    nvl(SUB_INVENTORY_CODE, 'NA')
  ) in (
    select 
      nvl(PLAN_ID, 0), 
      nvl(SR_INSTANCE_ID, 0), 
      nvl(ORGANIZATION_ID, 0), 
      nvl(SUB_INVENTORY_CODE, 'NA') 
    from 
      bec_raw_dl_ext.MSC_SUB_INVENTORIES 
    where 
      (
        nvl(PLAN_ID, 0), 
        nvl(SR_INSTANCE_ID, 0), 
        nvl(ORGANIZATION_ID, 0), 
        nvl(SUB_INVENTORY_CODE, 'NA'), 
        KCA_SEQ_ID
      ) in (
        select 
          nvl(PLAN_ID, 0), 
          nvl(SR_INSTANCE_ID, 0), 
          nvl(ORGANIZATION_ID, 0), 
          nvl(SUB_INVENTORY_CODE, 'NA'), 
          max(KCA_SEQ_ID) as KCA_SEQ_ID 
        from 
          bec_raw_dl_ext.MSC_SUB_INVENTORIES 
        group by 
          nvl(PLAN_ID, 0), 
          nvl(SR_INSTANCE_ID, 0), 
          nvl(ORGANIZATION_ID, 0), 
          nvl(SUB_INVENTORY_CODE, 'NA')
      ) 
      and kca_operation = 'DELETE'
  );

commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'msc_sub_inventories';