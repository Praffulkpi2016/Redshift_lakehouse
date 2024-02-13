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

delete from bec_ods.XXBEC_PEGGING_DATA
where (NVL(LEVEL_SEQ,0),NVL(PEGGING_ID,0),NVL(MSC_TRXN_ID,0),NVL(PARENT_PEGGING_ID,0),
NVL(TRANSACTION_ID,0),NVL(PLAN_ID,0),NVL(ORGANIZATION_ID,0),NVL(ITEM_ID,0)) in (
select NVL(stg.LEVEL_SEQ,0),NVL(stg.PEGGING_ID,0),NVL(stg.MSC_TRXN_ID,0),NVL(stg.PARENT_PEGGING_ID,0),
NVL(stg.TRANSACTION_ID,0),NVL(stg.PLAN_ID,0),NVL(stg.ORGANIZATION_ID,0),NVL(stg.ITEM_ID,0) from bec_ods.xxbec_pegging_data ods, bec_ods_stg.xxbec_pegging_data stg
where ods.LEVEL_SEQ = stg.LEVEL_SEQ
AND ods.PEGGING_ID = stg.PEGGING_ID
AND ods.MSC_TRXN_ID = stg.MSC_TRXN_ID
AND ods.PARENT_PEGGING_ID = stg.PARENT_PEGGING_ID
AND ods.TRANSACTION_ID = stg.TRANSACTION_ID
AND ods.PLAN_ID = stg.PLAN_ID
AND ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
AND ods.ITEM_ID = stg.ITEM_ID
 and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.xxbec_pegging_data
       (	level_seq,
	plan_id,
	organization_id,
	pegging_id,
	prev_pegging_id,
	parent_pegging_id,
	demand_id,
	transaction_id,
	msc_trxn_id,
	demand_qty,
	demand_date,
	item_id,
	item_org,
	pegged_qty,
	origination_name,
	end_pegged_qty,
	end_demand_qty,
	end_demand_date,
	end_item_org,
	end_origination_name,
	end_satisfied_date,
	end_disposition,
	order_type,
	end_demand_class,
	sr_instance_id,
	op_seq_num,
	item_desc_org,
	new_order_date,
	new_dock_date,
	new_start_date,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
	level_seq,
	plan_id,
	organization_id,
	pegging_id,
	prev_pegging_id,
	parent_pegging_id,
	demand_id,
	transaction_id,
	msc_trxn_id,
	demand_qty,
	demand_date,
	item_id,
	item_org,
	pegged_qty,
	origination_name,
	end_pegged_qty,
	end_demand_qty,
	end_demand_date,
	end_item_org,
	end_origination_name,
	end_satisfied_date,
	end_disposition,
	order_type,
	end_demand_class,
	sr_instance_id,
	op_seq_num,
	item_desc_org,
	new_order_date,
	new_dock_date,
	new_start_date,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.xxbec_pegging_data
	where kca_operation IN ('INSERT','UPDATE') 
	and ( NVL(LEVEL_SEQ,0),NVL(PEGGING_ID,0),NVL(MSC_TRXN_ID,0),NVL(PARENT_PEGGING_ID,0),
	NVL(TRANSACTION_ID,0),NVL(PLAN_ID,0),NVL(ORGANIZATION_ID,0),NVL(ITEM_ID,0),kca_seq_id) in 
	(select NVL(LEVEL_SEQ,0),NVL(PEGGING_ID,0),NVL(MSC_TRXN_ID,0),NVL(PARENT_PEGGING_ID,0),
	NVL(TRANSACTION_ID,0),NVL(PLAN_ID,0),NVL(ORGANIZATION_ID,0),NVL(ITEM_ID,0),max(kca_seq_id) from bec_ods_stg.xxbec_pegging_data 
     where kca_operation IN ('INSERT','UPDATE')
     group by NVL(LEVEL_SEQ,0),NVL(PEGGING_ID,0),NVL(MSC_TRXN_ID,0),NVL(PARENT_PEGGING_ID,0),
NVL(TRANSACTION_ID,0),NVL(PLAN_ID,0),NVL(ORGANIZATION_ID,0),NVL(ITEM_ID,0))
);

commit;

-- Soft delete
update bec_ods.xxbec_pegging_data set IS_DELETED_FLG = 'N';
commit;
update bec_ods.xxbec_pegging_data set IS_DELETED_FLG = 'Y'
where (NVL(LEVEL_SEQ,0),NVL(PEGGING_ID,0),NVL(MSC_TRXN_ID,0),NVL(PARENT_PEGGING_ID,0),NVL(TRANSACTION_ID,0),NVL(PLAN_ID,0),NVL(ORGANIZATION_ID,0),NVL(ITEM_ID,0))  in
(
select NVL(LEVEL_SEQ,0),NVL(PEGGING_ID,0),NVL(MSC_TRXN_ID,0),NVL(PARENT_PEGGING_ID,0),NVL(TRANSACTION_ID,0),NVL(PLAN_ID,0),NVL(ORGANIZATION_ID,0),NVL(ITEM_ID,0) from bec_raw_dl_ext.xxbec_pegging_data
where (NVL(LEVEL_SEQ,0),NVL(PEGGING_ID,0),NVL(MSC_TRXN_ID,0),NVL(PARENT_PEGGING_ID,0),NVL(TRANSACTION_ID,0),NVL(PLAN_ID,0),NVL(ORGANIZATION_ID,0),NVL(ITEM_ID,0),KCA_SEQ_ID)
in 
(
select NVL(LEVEL_SEQ,0),NVL(PEGGING_ID,0),NVL(MSC_TRXN_ID,0),NVL(PARENT_PEGGING_ID,0),NVL(TRANSACTION_ID,0),NVL(PLAN_ID,0),NVL(ORGANIZATION_ID,0),NVL(ITEM_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.xxbec_pegging_data
group by NVL(LEVEL_SEQ,0),NVL(PEGGING_ID,0),NVL(MSC_TRXN_ID,0),NVL(PARENT_PEGGING_ID,0),NVL(TRANSACTION_ID,0),NVL(PLAN_ID,0),NVL(ORGANIZATION_ID,0),NVL(ITEM_ID,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'xxbec_pegging_data';

commit;