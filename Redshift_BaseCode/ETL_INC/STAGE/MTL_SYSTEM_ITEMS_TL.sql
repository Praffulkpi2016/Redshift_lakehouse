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

TRUNCATE TABLE bec_ods_stg.MTL_SYSTEM_ITEMS_TL;

insert into	bec_ods_stg.MTL_SYSTEM_ITEMS_TL
    (inventory_item_id,
	organization_id,
	"language",
	source_lang,
	description,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	long_description,
	KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date)
(select
	inventory_item_id,
	organization_id,
	"language",
	source_lang,
	description,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	long_description,
	KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date
from
	bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,''),KCA_SEQ_ID) in 
	(select nvl(INVENTORY_ITEM_ID,0) AS INVENTORY_ITEM_ID ,  nvl(ORGANIZATION_ID,0) AS ORGANIZATION_ID,nvl(LANGUAGE,'') AS LANGUAGE ,max(KCA_SEQ_ID) from bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,''))
     and ( kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='mtl_system_items_tl')
	  
            )
	 );
END;