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

truncate
	table bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS;

insert
	into
	bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS
    (
	product_id 
	,product_name
	,product_rating
	,creation_date 
	,created_by 
	,last_update_date 
	,last_updated_by 
	,inventory_item_id 
	,KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date)
(
	select
		product_id 
	,product_name
	,product_rating
	,creation_date 
	,created_by 
	,last_update_date 
	,last_updated_by 
	,inventory_item_id 
		,KCA_OPERATION,
		KCA_SEQ_ID,
		kca_seq_date
	from
		bec_raw_dl_ext.XXBEC_PA_CONTRACT_PRODUCTS
	where
		kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and ( nvl(PRODUCT_ID, 0) ,
				nvl(INVENTORY_ITEM_ID, 0) ,
		KCA_SEQ_ID) in 
	(
		select
			nvl(PRODUCT_ID, 0) as PRODUCT_ID,
			nvl(INVENTORY_ITEM_ID, 0) as INVENTORY_ITEM_ID,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.XXBEC_PA_CONTRACT_PRODUCTS
		where
			kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(PRODUCT_ID, 0) ,
				nvl(INVENTORY_ITEM_ID, 0) )
		and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'xxbec_pa_contract_products'));
end;