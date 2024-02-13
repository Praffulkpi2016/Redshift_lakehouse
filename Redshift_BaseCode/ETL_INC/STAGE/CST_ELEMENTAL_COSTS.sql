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

truncate table bec_ods_stg.CST_ELEMENTAL_COSTS;

insert into	bec_ods_stg.CST_ELEMENTAL_COSTS
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
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.CST_ELEMENTAL_COSTS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID,kca_seq_id) in 
	(select COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID,max(kca_seq_id) from bec_raw_dl_ext.CST_ELEMENTAL_COSTS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by COST_UPDATE_ID,ORGANIZATION_ID,INVENTORY_ITEM_ID,COST_ELEMENT_ID)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'cst_elemental_costs')
);
end;