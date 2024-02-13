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

	DROP TABLE IF EXISTS bec_ods_stg.msc_system_items;

	CREATE TABLE bec_ods_stg.msc_system_items 
	DISTSTYLE AUTO
	SORTKEY (  SR_INSTANCE_ID, PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, last_update_date)
 AS 
	SELECT * FROM bec_raw_dl_ext.msc_system_items
	where kca_operation != 'DELETE' 
	and (SR_INSTANCE_ID,PLAN_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,last_update_date) in 
	(select SR_INSTANCE_ID,PLAN_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,max(last_update_date) from bec_raw_dl_ext.msc_system_items 
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
	group by SR_INSTANCE_ID,PLAN_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID);

	END;