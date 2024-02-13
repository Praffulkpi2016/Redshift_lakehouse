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
	table bec_ods_stg.CS_BILLING_TYPE_CATEGORIES;

COMMIT;

insert
	into
	bec_ods_stg.CS_BILLING_TYPE_CATEGORIES
(	
	BILLING_TYPE, 
	BILLING_CATEGORY, 
	ROLLUP_ITEM_ID, 
	START_DATE_ACTIVE, 
	END_DATE_ACTIVE, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_LOGIN, 
	SECURITY_GROUP_ID, 
	SEEDED_FLAG, 
	ZD_EDITION_NAME, 
	ZD_SYNC,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		BILLING_TYPE, 
		BILLING_CATEGORY, 
		ROLLUP_ITEM_ID, 
		START_DATE_ACTIVE, 
		END_DATE_ACTIVE, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		SECURITY_GROUP_ID, 
		SEEDED_FLAG, 
		ZD_EDITION_NAME, 
		ZD_SYNC,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.CS_BILLING_TYPE_CATEGORIES
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(BILLING_TYPE, 'NA'),
		kca_seq_id) in 
(
		select
			nvl(BILLING_TYPE,'NA') as BILLING_TYPE,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.CS_BILLING_TYPE_CATEGORIES
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(BILLING_TYPE, 'NA'))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'cs_billing_type_categories')
);
end;