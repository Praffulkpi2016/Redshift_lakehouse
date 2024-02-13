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

delete from bec_ods.CST_STANDARD_COSTS
where (COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID) in (
select stg.COST_UPDATE_ID,stg.INVENTORY_ITEM_ID,stg.ORGANIZATION_ID
from bec_ods.CST_STANDARD_COSTS ods, bec_ods_stg.CST_STANDARD_COSTS stg
where ods.COST_UPDATE_ID = stg.COST_UPDATE_ID
and  ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
and ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.CST_STANDARD_COSTS
       (cost_update_id,
	inventory_item_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	standard_cost_revision_date,
	standard_cost,
	inventory_adjustment_quantity,
	inventory_adjustment_value,
	intransit_adjustment_quantity,
	intransit_adjustment_value,
	wip_adjustment_quantity,
	wip_adjustment_value,
	last_cost_update_id,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)	
(
	select
		cost_update_id,
	inventory_item_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	standard_cost_revision_date,
	standard_cost,
	inventory_adjustment_quantity,
	inventory_adjustment_value,
	intransit_adjustment_quantity,
	intransit_adjustment_value,
	wip_adjustment_quantity,
	wip_adjustment_value,
	last_cost_update_id,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.CST_STANDARD_COSTS
	where kca_operation IN ('INSERT','UPDATE') 
	and (COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,kca_seq_id) in 
	(select COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,max(kca_seq_id) from bec_ods_stg.CST_STANDARD_COSTS 
     where kca_operation IN ('INSERT','UPDATE')
     group by COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID)
);

commit;

-- Soft delete
update bec_ods.CST_STANDARD_COSTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CST_STANDARD_COSTS set IS_DELETED_FLG = 'Y'
where (COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID)  in
(
select COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID from bec_raw_dl_ext.CST_STANDARD_COSTS
where (COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,KCA_SEQ_ID)
in 
(
select COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CST_STANDARD_COSTS
group by COST_UPDATE_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'cst_standard_costs';

commit;