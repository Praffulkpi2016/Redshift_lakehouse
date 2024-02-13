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
delete from bec_ods.mtl_item_categories
where (INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID ) in (
select stg.INVENTORY_ITEM_ID,stg.ORGANIZATION_ID,stg.CATEGORY_SET_ID  from bec_ods.mtl_item_categories ods, bec_ods_stg.mtl_item_categories stg
where ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID 
and ods.ORGANIZATION_ID=stg.ORGANIZATION_ID and ods.CATEGORY_SET_ID=stg.CATEGORY_SET_ID  
and stg.kca_operation in ('INSERT','UPDATE'));

commit;

-- Insert records
insert into bec_ods.mtl_item_categories
(INVENTORY_ITEM_ID
,ORGANIZATION_ID
,CATEGORY_SET_ID
,CATEGORY_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,WH_UPDATE_DATE
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID,
	kca_seq_date)
(
select INVENTORY_ITEM_ID,
ORGANIZATION_ID,
CATEGORY_SET_ID,
CATEGORY_ID,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
REQUEST_ID,
PROGRAM_APPLICATION_ID,
PROGRAM_ID,
PROGRAM_UPDATE_DATE,
WH_UPDATE_DATE,
KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from bec_ods_stg.mtl_item_categories
where kca_operation IN ('INSERT','UPDATE') and (INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID ,kca_seq_id) in 
(select INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID ,max(kca_seq_id) 
from bec_ods_stg.mtl_item_categories 
where kca_operation IN ('INSERT','UPDATE')
group by INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID )
);
commit;
 

-- Soft delete
update bec_ods.mtl_item_categories set IS_DELETED_FLG = 'N';
commit;
update bec_ods.mtl_item_categories set IS_DELETED_FLG = 'Y'
where (INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID)  in
(
select INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID from bec_raw_dl_ext.mtl_item_categories
where (INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID,KCA_SEQ_ID)
in 
(
select INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.mtl_item_categories
group by INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='mtl_item_categories';
commit;