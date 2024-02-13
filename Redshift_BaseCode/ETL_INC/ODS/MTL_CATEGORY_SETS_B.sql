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

delete from bec_ods.mtl_category_sets_b
where CATEGORY_SET_ID in (
select stg.CATEGORY_SET_ID from bec_ods.mtl_category_sets_b ods, bec_ods_stg.mtl_category_sets_b stg
where ods.CATEGORY_SET_ID = stg.CATEGORY_SET_ID and stg.kca_operation in ('INSERT','UPDATE'));

commit; 
-- Insert records

insert into bec_ods.MTL_CATEGORY_SETS_B
(
CATEGORY_SET_ID      
,STRUCTURE_ID    
,VALIDATE_FLAG    
,CONTROL_LEVEL    
 ,LAST_UPDATE_DATE   
,LAST_UPDATED_BY    
,CREATION_DATE    
,CREATED_BY    
,LAST_UPDATE_LOGIN    
,REQUEST_ID    
,PROGRAM_APPLICATION_ID   
,PROGRAM_ID    
,PROGRAM_UPDATE_DATE    
,DEFAULT_CATEGORY_ID    
,MULT_ITEM_CAT_ASSIGN_FLAG    
,CONTROL_LEVEL_UPDATEABLE_FLAG    
,MULT_ITEM_CAT_UPDATEABLE_FLAG   
,HIERARCHY_ENABLED    
,VALIDATE_FLAG_UPDATEABLE_FLAG   
,USER_CREATION_ALLOWED_FLAG    
,RAISE_ITEM_CAT_ASSIGN_EVENT    
,RAISE_ALT_CAT_HIER_CHG_EVENT    
,RAISE_CATALOG_CAT_CHG_EVENT
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,IS_DELETED_FLG
,kca_seq_id,
	kca_seq_date
) 
SELECT CATEGORY_SET_ID  
 ,STRUCTURE_ID    
,VALIDATE_FLAG    
,CONTROL_LEVEL    
 ,LAST_UPDATE_DATE   
,LAST_UPDATED_BY    
,CREATION_DATE    
,CREATED_BY    
,LAST_UPDATE_LOGIN    
,REQUEST_ID    
,PROGRAM_APPLICATION_ID   
,PROGRAM_ID    
,PROGRAM_UPDATE_DATE    
,DEFAULT_CATEGORY_ID    
,MULT_ITEM_CAT_ASSIGN_FLAG    
,CONTROL_LEVEL_UPDATEABLE_FLAG    
,MULT_ITEM_CAT_UPDATEABLE_FLAG   
,HIERARCHY_ENABLED    
,VALIDATE_FLAG_UPDATEABLE_FLAG   
,USER_CREATION_ALLOWED_FLAG    
,RAISE_ITEM_CAT_ASSIGN_EVENT    
,RAISE_ALT_CAT_HIER_CHG_EVENT    
,RAISE_CATALOG_CAT_CHG_EVENT
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
FROM bec_ods_stg.mtl_category_sets_b
where kca_operation IN ('INSERT','UPDATE') and (CATEGORY_SET_ID,kca_seq_id) in (select CATEGORY_SET_ID,max(kca_seq_id) 
from bec_ods_stg.mtl_category_sets_b 
where kca_operation IN ('INSERT','UPDATE')
group by CATEGORY_SET_ID);

commit;
 
-- Soft delete
update bec_ods.mtl_category_sets_b set IS_DELETED_FLG = 'N';
commit;
update bec_ods.mtl_category_sets_b set IS_DELETED_FLG = 'Y'
where (CATEGORY_SET_ID)  in
(
select CATEGORY_SET_ID from bec_raw_dl_ext.mtl_category_sets_b
where (CATEGORY_SET_ID,KCA_SEQ_ID)
in 
(
select CATEGORY_SET_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.mtl_category_sets_b
group by CATEGORY_SET_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info 
set last_refresh_date = getdate()
where ods_table_name= 'mtl_category_sets_b';
commit;
