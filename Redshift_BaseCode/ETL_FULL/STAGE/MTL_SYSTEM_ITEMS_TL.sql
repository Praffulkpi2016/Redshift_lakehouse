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
	DROP TABLE IF EXISTS bec_ods_stg.MTL_SYSTEM_ITEMS_TL; 

	CREATE TABLE bec_ods_stg.MTL_SYSTEM_ITEMS_TL 
	DISTSTYLE AUTO
SORTKEY (INVENTORY_ITEM_ID, ORGANIZATION_ID, last_update_date)
	AS (SELECT * FROM bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL
	WHERE kca_operation != 'DELETE' and 
	(nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,''),last_update_date)
	in ( select nvl(INVENTORY_ITEM_ID,0) AS INVENTORY_ITEM_ID ,  nvl(ORGANIZATION_ID,0) AS ORGANIZATION_ID,nvl(LANGUAGE,'') AS LANGUAGE,
	max(last_update_date) from bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL 
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'') = ''
	group by  nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,'')
	)
	);
	
	END;
