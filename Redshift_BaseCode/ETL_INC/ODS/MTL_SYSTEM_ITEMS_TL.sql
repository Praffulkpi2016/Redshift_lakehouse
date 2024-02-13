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

delete from bec_ods.MTL_SYSTEM_ITEMS_TL
where (nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,'')) in (
select nvl(stg.INVENTORY_ITEM_ID,0) AS INVENTORY_ITEM_ID ,  nvl(stg.ORGANIZATION_ID,0) 
AS ORGANIZATION_ID,nvl(stg.LANGUAGE,'') AS LANGUAGE
 
from bec_ods.MTL_SYSTEM_ITEMS_TL ods, bec_ods_stg.MTL_SYSTEM_ITEMS_TL stg
where nvl(ods.INVENTORY_ITEM_ID,0) = nvl(stg.INVENTORY_ITEM_ID,0) AND
      nvl(ods.ORGANIZATION_ID,0) = nvl(stg.ORGANIZATION_ID,0) AND
      nvl(ods.LANGUAGE,'') = nvl(stg.LANGUAGE,'')  and 
      stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MTL_SYSTEM_ITEMS_TL
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
	IS_DELETED_FLG,
	kca_seq_id,
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
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.MTL_SYSTEM_ITEMS_TL
where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,''),KCA_SEQ_ID) in 
	(select nvl(INVENTORY_ITEM_ID,0) AS INVENTORY_ITEM_ID ,  nvl(ORGANIZATION_ID,0) AS ORGANIZATION_ID,nvl(LANGUAGE,'') AS LANGUAGE,max(KCA_SEQ_ID) from bec_ods_stg.MTL_SYSTEM_ITEMS_TL 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,''))	
	);

commit;

 

-- Soft delete
update bec_ods.MTL_SYSTEM_ITEMS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_SYSTEM_ITEMS_TL set IS_DELETED_FLG = 'Y'
where (nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,''))  in
(
select nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,'') from bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL
where (nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,''),KCA_SEQ_ID)
in 
(
select nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,''),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL
group by nvl(INVENTORY_ITEM_ID,0) ,nvl(ORGANIZATION_ID,0),nvl(LANGUAGE,'')
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
	ods_table_name = 'mtl_system_items_tl';

commit;