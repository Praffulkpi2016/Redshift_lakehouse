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
	table bec_ods_stg.OE_ORDER_SOURCES;

insert
	into
	bec_ods_stg.OE_ORDER_SOURCES
    (order_source_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	"name",
	description,
	enabled_flag,
	create_customers_flag,
	use_ids_flag,
	aia_enabled_flag,
	zd_edition_name, 
	KCA_OPERATION,
	KCA_SEQ_ID
	,kca_seq_date)
(
	select
	order_source_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	"name",
	description,
	enabled_flag,
	create_customers_flag,
	use_ids_flag,
	aia_enabled_flag,
	zd_edition_name, 
		KCA_OPERATION,
		KCA_SEQ_ID
		,kca_seq_date
	from
		bec_raw_dl_ext.OE_ORDER_SOURCES
	where
		kca_operation != 'DELETE' and nvl(kca_seq_id,'') != ''
		and (nvl(ORDER_SOURCE_ID, 0), 
		KCA_SEQ_ID) in 
	(
		select
			nvl(ORDER_SOURCE_ID, 0) as ORDER_SOURCE_ID ,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.OE_ORDER_SOURCES
		where
			kca_operation != 'DELETE' and nvl(kca_seq_id,'') != ''
		group by
			ORDER_SOURCE_ID )
		and (kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'oe_order_sources')
)
);
end;