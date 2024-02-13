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
	
	TRUNCATE TABLE bec_ods_stg.MTL_ITEM_SUB_DEFAULTS;
	
	insert into	bec_ods_stg.MTL_ITEM_SUB_DEFAULTS
    (
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
		KCA_OPERATION,
		kca_seq_id,
	kca_seq_date)
	(select
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
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
		from
		bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS 
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
		and (nvl(INVENTORY_ITEM_ID, 0) ,
			nvl(ORGANIZATION_ID, 0) ,
			nvl(SUBINVENTORY_CODE, 'NA') ,
		nvl(DEFAULT_TYPE, 0) ,KCA_SEQ_ID) in 
		(select nvl(INVENTORY_ITEM_ID, 0) as INVENTORY_ITEM_ID ,
			nvl(ORGANIZATION_ID, 0) as ORGANIZATION_ID ,
			nvl(SUBINVENTORY_CODE, 'NA') as SUBINVENTORY_CODE ,
			nvl(DEFAULT_TYPE, 0) as DEFAULT_TYPE,max(KCA_SEQ_ID) from bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS 
			where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
			group by nvl(INVENTORY_ITEM_ID, 0) ,
			nvl(ORGANIZATION_ID, 0) ,
			nvl(SUBINVENTORY_CODE, 'NA') ,
		nvl(DEFAULT_TYPE, 0))
		and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='mtl_item_sub_defaults')	
	);
	END;	