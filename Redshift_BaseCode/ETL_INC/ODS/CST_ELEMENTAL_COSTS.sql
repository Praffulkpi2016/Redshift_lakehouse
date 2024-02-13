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

delete from bec_ods.CST_ELEMENTAL_COSTS
where (COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID) in (
select stg.COST_UPDATE_ID,stg.ORGANIZATION_ID,stg.INVENTORY_ITEM_ID,stg.COST_ELEMENT_ID from bec_ods.CST_ELEMENTAL_COSTS ods, bec_ods_stg.CST_ELEMENTAL_COSTS stg
where ods.COST_UPDATE_ID = stg.COST_UPDATE_ID and ods.ORGANIZATION_ID = stg.ORGANIZATION_ID and ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID and ods.COST_ELEMENT_ID = stg.COST_ELEMENT_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.CST_ELEMENTAL_COSTS
       (COST_UPDATE_ID,
		ORGANIZATION_ID,
		INVENTORY_ITEM_ID,
		COST_ELEMENT_ID,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		STANDARD_COST,
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		COST_UPDATE_ID,
		ORGANIZATION_ID,
		INVENTORY_ITEM_ID,
		COST_ELEMENT_ID,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		STANDARD_COST,
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.CST_ELEMENTAL_COSTS
	where kca_operation IN ('INSERT','UPDATE') 
	and (COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID,kca_seq_id) in 
	(select COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID,max(kca_seq_id) from bec_ods_stg.CST_ELEMENTAL_COSTS 
     where kca_operation IN ('INSERT','UPDATE')
     group by COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID)
);

commit;

-- Soft delete
update bec_ods.CST_ELEMENTAL_COSTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CST_ELEMENTAL_COSTS set IS_DELETED_FLG = 'Y'
where (COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID)  in
(
select COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID from bec_raw_dl_ext.CST_ELEMENTAL_COSTS
where (COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID,KCA_SEQ_ID)
in 
(
select COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CST_ELEMENTAL_COSTS
group by COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'cst_elemental_costs';

commit;