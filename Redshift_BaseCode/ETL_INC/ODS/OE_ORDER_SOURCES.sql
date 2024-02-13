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
	bec_ods.OE_ORDER_SOURCES
where
	(
	nvl(ORDER_SOURCE_ID, 0) 
	) in 
	(
	select
		NVL(stg.ORDER_SOURCE_ID, 0) as ORDER_SOURCE_ID 
	from
		bec_ods.OE_ORDER_SOURCES ods,
		bec_ods_stg.OE_ORDER_SOURCES stg
	where
		    NVL(ods.ORDER_SOURCE_ID, 0) = NVL(stg.ORDER_SOURCE_ID, 0) 
					and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.OE_ORDER_SOURCES (
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
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.OE_ORDER_SOURCES
	where
		kca_operation in ('INSERT','UPDATE')
		and (
		nvl(ORDER_SOURCE_ID, 0) ,
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(ORDER_SOURCE_ID, 0) as ORDER_SOURCE_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.OE_ORDER_SOURCES
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			ORDER_SOURCE_ID 
			)	
	);

commit;

-- Soft delete
update bec_ods.OE_ORDER_SOURCES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OE_ORDER_SOURCES set IS_DELETED_FLG = 'Y'
where (ORDER_SOURCE_ID)  in
(
select ORDER_SOURCE_ID from bec_raw_dl_ext.OE_ORDER_SOURCES
where (ORDER_SOURCE_ID,KCA_SEQ_ID)
in 
(
select ORDER_SOURCE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OE_ORDER_SOURCES
group by ORDER_SOURCE_ID
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
	ods_table_name = 'oe_order_sources';

commit;