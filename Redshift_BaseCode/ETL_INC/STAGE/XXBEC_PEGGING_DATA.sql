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

truncate table bec_ods_stg.xxbec_pegging_data;

insert into	bec_ods_stg.xxbec_pegging_data
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
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.XXBEC_PEGGING_DATA
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (LEVEL_SEQ,PEGGING_ID,MSC_TRXN_ID,PARENT_PEGGING_ID,TRANSACTION_ID,PLAN_ID,ORGANIZATION_ID,ITEM_ID,kca_seq_id) in 
	(select LEVEL_SEQ,PEGGING_ID,MSC_TRXN_ID,PARENT_PEGGING_ID,TRANSACTION_ID,PLAN_ID,ORGANIZATION_ID,ITEM_ID,max(kca_seq_id) from bec_raw_dl_ext.xxbec_pegging_data 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by LEVEL_SEQ,PEGGING_ID,MSC_TRXN_ID,PARENT_PEGGING_ID,TRANSACTION_ID,PLAN_ID,ORGANIZATION_ID,ITEM_ID)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'xxbec_pegging_data')
);
end;