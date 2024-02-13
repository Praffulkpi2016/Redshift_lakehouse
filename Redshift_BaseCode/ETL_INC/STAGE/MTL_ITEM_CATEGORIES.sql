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

truncate
	table bec_ods_stg.mtl_item_categories;

insert
	into
	bec_ods_stg.mtl_item_categories
(INVENTORY_ITEM_ID,
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
KCA_OPERATION
,kca_seq_id,
	kca_seq_date
)
(
	select
	INVENTORY_ITEM_ID,
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
KCA_OPERATION
,kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.mtl_item_categories
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' and (INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID ,kca_seq_id) in 
(select INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID ,max(kca_seq_id) from bec_raw_dl_ext.mtl_item_categories 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by INVENTORY_ITEM_ID,ORGANIZATION_ID,CATEGORY_SET_ID )  

and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_item_categories')
		 
            )	


);		
		
end;
