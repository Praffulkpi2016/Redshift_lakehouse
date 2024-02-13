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
	bec_ods.XXBEC_PA_CONTRACT_PRODUCTS
where
	(nvl(PRODUCT_ID, 0),
	nvl(INVENTORY_ITEM_ID, 0)) in 
	(
	select
		NVL(stg.PRODUCT_ID, 0) as PRODUCT_ID,
		NVL(stg.INVENTORY_ITEM_ID, 0) as INVENTORY_ITEM_ID
	from
		bec_ods.XXBEC_PA_CONTRACT_PRODUCTS ods,
		bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS stg
	where
		NVL(ods.PRODUCT_ID, 0) = NVL(stg.PRODUCT_ID, 0)
			and 
		NVL(ods.INVENTORY_ITEM_ID, 0) = NVL(stg.INVENTORY_ITEM_ID, 0)
				and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.XXBEC_PA_CONTRACT_PRODUCTS (
	product_id 
	,product_name
	,product_rating
	,creation_date 
	,created_by 
	,last_update_date 
	,last_updated_by 
	,inventory_item_id 
	,kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date)
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
		,kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(PRODUCT_ID, 0),
		NVL(INVENTORY_ITEM_ID, 0),
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(PRODUCT_ID, 0) as PRODUCT_ID,
			NVL(INVENTORY_ITEM_ID, 0) as INVENTORY_ITEM_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(PRODUCT_ID, 0) ,
			NVL(INVENTORY_ITEM_ID, 0)
			)	
	);

commit;

-- Soft delete
update bec_ods.XXBEC_PA_CONTRACT_PRODUCTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.XXBEC_PA_CONTRACT_PRODUCTS set IS_DELETED_FLG = 'Y'
where (NVL(PRODUCT_ID, 0),NVL(INVENTORY_ITEM_ID, 0))  in
(
select NVL(PRODUCT_ID, 0),NVL(INVENTORY_ITEM_ID, 0) from bec_raw_dl_ext.XXBEC_PA_CONTRACT_PRODUCTS
where (NVL(PRODUCT_ID, 0),NVL(INVENTORY_ITEM_ID, 0),KCA_SEQ_ID)
in 
(
select NVL(PRODUCT_ID, 0),NVL(INVENTORY_ITEM_ID, 0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.XXBEC_PA_CONTRACT_PRODUCTS
group by NVL(PRODUCT_ID, 0),NVL(INVENTORY_ITEM_ID, 0)
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
	ods_table_name = 'xxbec_pa_contract_products';

commit;