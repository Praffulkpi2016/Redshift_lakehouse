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

delete from bec_ods.MSC_BOMS
where (PLAN_ID ,SR_INSTANCE_ID ,BILL_SEQUENCE_ID) in (
select stg.PLAN_ID ,stg.SR_INSTANCE_ID,stg.BILL_SEQUENCE_ID
from bec_ods.MSC_BOMS ods, bec_ods_stg.MSC_BOMS stg
where ods.PLAN_ID = stg.PLAN_ID
  and ods.SR_INSTANCE_ID = stg.SR_INSTANCE_ID
  and ods.BILL_SEQUENCE_ID = stg.BILL_SEQUENCE_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;


-- Insert records

insert into	bec_ods.MSC_BOMS
    (
	    PLAN_ID
,      BILL_SEQUENCE_ID
,      SR_INSTANCE_ID
,      ASSEMBLY_TYPE
,      ALTERNATE_BOM_DESIGNATOR
,      SPECIFIC_ASSEMBLY_COMMENT
,      PENDING_FROM_ECN
,      SCALING_TYPE
,      ASSEMBLY_QUANTITY
,      UOM
,      ORGANIZATION_ID
,      ASSEMBLY_ITEM_ID
,      REFRESH_NUMBER
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      YIELDING_OP_SEQ_NUM
,      NEW_PLAN_ID
,      NEW_PLAN_LIST
,      APPLIED
,      SIMULATION_SET_ID
,      REPAIRABLE
,      ACTIVITY_ITEM_ID
,"alternate_bom_designator#1"
,	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	    PLAN_ID
,      BILL_SEQUENCE_ID
,      SR_INSTANCE_ID
,      ASSEMBLY_TYPE
,      ALTERNATE_BOM_DESIGNATOR
,      SPECIFIC_ASSEMBLY_COMMENT
,      PENDING_FROM_ECN
,      SCALING_TYPE
,      ASSEMBLY_QUANTITY
,      UOM
,      ORGANIZATION_ID
,      ASSEMBLY_ITEM_ID
,      REFRESH_NUMBER
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      YIELDING_OP_SEQ_NUM
,      NEW_PLAN_ID
,      NEW_PLAN_LIST
,      APPLIED
,      SIMULATION_SET_ID
,      REPAIRABLE
,      ACTIVITY_ITEM_ID
,"alternate_bom_designator#1"
,	KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.MSC_BOMS
where kca_operation IN ('INSERT','UPDATE') 
	and (PLAN_ID ,SR_INSTANCE_ID ,BILL_SEQUENCE_ID,KCA_SEQ_ID) in 
	(select PLAN_ID ,SR_INSTANCE_ID ,BILL_SEQUENCE_ID,max(KCA_SEQ_ID) from bec_ods_stg.MSC_BOMS 
     where kca_operation IN ('INSERT','UPDATE')
     group by PLAN_ID ,SR_INSTANCE_ID ,BILL_SEQUENCE_ID)
);

commit;

-- Soft delete
update bec_ods.MSC_BOMS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MSC_BOMS set IS_DELETED_FLG = 'Y'
where (PLAN_ID ,SR_INSTANCE_ID ,BILL_SEQUENCE_ID)  in
(
select PLAN_ID ,SR_INSTANCE_ID ,BILL_SEQUENCE_ID from bec_raw_dl_ext.MSC_BOMS
where (PLAN_ID ,SR_INSTANCE_ID ,BILL_SEQUENCE_ID,KCA_SEQ_ID)
in 
(
select PLAN_ID ,SR_INSTANCE_ID ,BILL_SEQUENCE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MSC_BOMS
group by PLAN_ID ,SR_INSTANCE_ID ,BILL_SEQUENCE_ID
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
	ods_table_name = 'msc_boms';
 