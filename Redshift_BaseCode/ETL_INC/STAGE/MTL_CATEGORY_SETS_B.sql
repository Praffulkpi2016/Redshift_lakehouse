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
BEGIN;
TRUNCATE TABLE bec_ods_stg.MTL_CATEGORY_SETS_B;

insert into bec_ods_stg.MTL_CATEGORY_SETS_B
(
CATEGORY_SET_ID  
--,CATEGORY_SET_NAME    
,STRUCTURE_ID    
,VALIDATE_FLAG    
,CONTROL_LEVEL    
--,DESCRIPTION    
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
,kca_seq_id,
	kca_seq_date
) 
SELECT CATEGORY_SET_ID  
--,CATEGORY_SET_NAME    
,STRUCTURE_ID    
,VALIDATE_FLAG    
,CONTROL_LEVEL    
--,DESCRIPTION    
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
,kca_seq_id,
	kca_seq_date
FROM bec_raw_dl_ext.mtl_category_sets_b 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' and (CATEGORY_SET_ID,kca_seq_id) in (select CATEGORY_SET_ID,max(kca_seq_id) from bec_raw_dl_ext.mtl_category_sets_b 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by CATEGORY_SET_ID)
and 
( kca_seq_date > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'mtl_category_sets_b')
 
            )

;


end;