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
	
	delete
	from
	bec_ods.MTL_ITEM_SUB_DEFAULTS
	where
	(
		nvl(INVENTORY_ITEM_ID, 0) ,
		nvl(ORGANIZATION_ID, 0) ,
		nvl(SUBINVENTORY_CODE, 'NA') ,
		nvl(DEFAULT_TYPE, 0) 
		
	) in 
	(
		select
		NVL(stg.INVENTORY_ITEM_ID,0) AS INVENTORY_ITEM_ID ,
		nvl(stg.ORGANIZATION_ID, 0) as ORGANIZATION_ID ,
		nvl(stg.SUBINVENTORY_CODE, 'NA') as SUBINVENTORY_CODE ,
		nvl(stg.DEFAULT_TYPE, 0) as DEFAULT_TYPE
		from
		bec_ods.MTL_ITEM_SUB_DEFAULTS ods,
		bec_ods_stg.MTL_ITEM_SUB_DEFAULTS stg
		where
		NVL(ods.INVENTORY_ITEM_ID,0) = NVL(stg.INVENTORY_ITEM_ID,0) AND
		NVL(ods.ORGANIZATION_ID,0) = NVL(stg.ORGANIZATION_ID,0) AND
		NVL(ods.SUBINVENTORY_CODE,'NA') = NVL(stg.SUBINVENTORY_CODE,'NA') AND
		NVL(ods.DEFAULT_TYPE,0) = NVL(stg.DEFAULT_TYPE,0)  
		and stg.kca_operation in ('INSERT', 'UPDATE')
	);
	
	commit;
	-- Insert records
	
	insert
	into bec_ods.MTL_ITEM_SUB_DEFAULTS  (
		inventory_item_id,
		organization_id,
		subinventory_code,
		default_type,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
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
		inventory_item_id,
		organization_id,
		subinventory_code,
		default_type,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		request_id,
		program_application_id,
		program_id,
		program_update_date,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
		from
		bec_ods_stg.MTL_ITEM_SUB_DEFAULTS
		where
		kca_operation IN ('INSERT','UPDATE')
		and (
			nvl(INVENTORY_ITEM_ID, 0) ,
			nvl(ORGANIZATION_ID, 0) ,
			nvl(SUBINVENTORY_CODE, 'NA') ,
			nvl(DEFAULT_TYPE, 0), 
			KCA_SEQ_ID
		) in 
		(
			select
			nvl(INVENTORY_ITEM_ID, 0) as INVENTORY_ITEM_ID ,
			nvl(ORGANIZATION_ID, 0) as ORGANIZATION_ID ,
			nvl(SUBINVENTORY_CODE, 'NA') as SUBINVENTORY_CODE ,
			nvl(DEFAULT_TYPE, 0) as DEFAULT_TYPE ,
			max(KCA_SEQ_ID)
			from
			bec_ods_stg.MTL_ITEM_SUB_DEFAULTS
			where
			kca_operation IN ('INSERT','UPDATE')
			group by
			nvl(INVENTORY_ITEM_ID, 0) ,
			nvl(ORGANIZATION_ID, 0) ,
			nvl(SUBINVENTORY_CODE, 'NA') ,
			nvl(DEFAULT_TYPE, 0)   
		)	
	);
	
	commit;
	
	-- Soft delete
update bec_ods.MTL_ITEM_SUB_DEFAULTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_ITEM_SUB_DEFAULTS set IS_DELETED_FLG = 'Y'
where (nvl(INVENTORY_ITEM_ID, 0) ,nvl(ORGANIZATION_ID, 0) ,nvl(SUBINVENTORY_CODE, 'NA') ,nvl(DEFAULT_TYPE, 0)  )  in
(
select nvl(INVENTORY_ITEM_ID, 0) ,nvl(ORGANIZATION_ID, 0) ,nvl(SUBINVENTORY_CODE, 'NA') ,nvl(DEFAULT_TYPE, 0)   from bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS
where (nvl(INVENTORY_ITEM_ID, 0) ,nvl(ORGANIZATION_ID, 0) ,nvl(SUBINVENTORY_CODE, 'NA') ,nvl(DEFAULT_TYPE, 0)  ,KCA_SEQ_ID)
in 
(
select nvl(INVENTORY_ITEM_ID, 0) ,nvl(ORGANIZATION_ID, 0) ,nvl(SUBINVENTORY_CODE, 'NA') ,nvl(DEFAULT_TYPE, 0)  ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS
group by nvl(INVENTORY_ITEM_ID, 0) ,nvl(ORGANIZATION_ID, 0) ,nvl(SUBINVENTORY_CODE, 'NA') ,nvl(DEFAULT_TYPE, 0)  
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
	ods_table_name = 'mtl_item_sub_defaults';
	
	commit;	