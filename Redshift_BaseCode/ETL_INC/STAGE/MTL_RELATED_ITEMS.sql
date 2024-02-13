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

TRUNCATE TABLE bec_ods_stg.MTL_RELATED_ITEMS;

insert into	bec_ods_stg.MTL_RELATED_ITEMS
   (
     inventory_item_id,
	organization_id,
	related_item_id,
	relationship_type_id,
	reciprocal_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	object_version_number,
	start_date,
	end_date,
	attr_context,
	attr_char1,
	attr_char2,
	attr_char3,
	attr_char4,
	attr_char5,
	attr_char6,
	attr_char7,
	attr_char8,
	attr_char9,
	attr_char10,
	attr_num1,
	attr_num2,
	attr_num3,
	attr_num4,
	attr_num5,
	attr_num6,
	attr_num7,
	attr_num8,
	attr_num9,
	attr_num10,
	attr_date1,
	attr_date2,
	attr_date3,
	attr_date4,
	attr_date5,
	attr_date6,
	attr_date7,
	attr_date8,
	attr_date9,
	attr_date10,
	planning_enabled_flag,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(select
	inventory_item_id,
	organization_id,
	related_item_id,
	relationship_type_id,
	reciprocal_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	object_version_number,
	start_date,
	end_date,
	attr_context,
	attr_char1,
	attr_char2,
	attr_char3,
	attr_char4,
	attr_char5,
	attr_char6,
	attr_char7,
	attr_char8,
	attr_char9,
	attr_char10,
	attr_num1,
	attr_num2,
	attr_num3,
	attr_num4,
	attr_num5,
	attr_num6,
	attr_num7,
	attr_num8,
	attr_num9,
	attr_num10,
	attr_date1,
	attr_date2,
	attr_date3,
	attr_date4,
	attr_date5,
	attr_date6,
	attr_date7,
	attr_date8,
	attr_date9,
	attr_date10,
	planning_enabled_flag,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.MTL_RELATED_ITEMS
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(INVENTORY_ITEM_ID,0),	nvl(RELATED_ITEM_ID,0),nvl(RELATIONSHIP_TYPE_ID,0),	nvl(ORGANIZATION_ID,0) ,KCA_SEQ_ID) in 
	(select nvl(INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID, 
		nvl(RELATED_ITEM_ID,0) as RELATED_ITEM_ID,
		nvl(RELATIONSHIP_TYPE_ID,0) as RELATIONSHIP_TYPE_ID, 
		nvl(ORGANIZATION_ID,0) as ORGANIZATION_ID, max(KCA_SEQ_ID) from bec_raw_dl_ext.MTL_RELATED_ITEMS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(INVENTORY_ITEM_ID,0),	nvl(RELATED_ITEM_ID,0),nvl(RELATIONSHIP_TYPE_ID,0),	nvl(ORGANIZATION_ID,0)  )
     and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='mtl_related_items')
	 );
END;

