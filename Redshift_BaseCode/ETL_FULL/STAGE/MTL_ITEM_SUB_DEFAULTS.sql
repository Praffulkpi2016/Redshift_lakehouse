/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents full load approach for stage.
	# File Version: KPI v1.0
*/
BEGIN; 
	DROP TABLE IF EXISTS bec_ods_stg.MTL_ITEM_SUB_DEFAULTS;
	
	CREATE TABLE bec_ods_stg.MTL_ITEM_SUB_DEFAULTS 
	DISTKEY (INVENTORY_ITEM_ID) 
SORTKEY (
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  SUBINVENTORY_CODE,
  DEFAULT_TYPE,
  last_update_date
)
AS (SELECT * FROM bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS
	where kca_operation != 'DELETE' and ( 
		nvl(INVENTORY_ITEM_ID, 0) ,
		nvl(ORGANIZATION_ID, 0) ,
		nvl(SUBINVENTORY_CODE, 'NA') ,
	nvl(DEFAULT_TYPE, 0) ,last_update_date) 
	in (select nvl(INVENTORY_ITEM_ID, 0) as INVENTORY_ITEM_ID ,
		nvl(ORGANIZATION_ID, 0) as ORGANIZATION_ID ,
		nvl(SUBINVENTORY_CODE, 'NA') as SUBINVENTORY_CODE ,
		nvl(DEFAULT_TYPE, 0) as DEFAULT_TYPE ,max(last_update_date) from bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS 
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
		group by nvl(INVENTORY_ITEM_ID, 0) ,
		nvl(ORGANIZATION_ID, 0) ,
		nvl(SUBINVENTORY_CODE, 'NA') ,
	nvl(DEFAULT_TYPE, 0) ) );
	
END;
