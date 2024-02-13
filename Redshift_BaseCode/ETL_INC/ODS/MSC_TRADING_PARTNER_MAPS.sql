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
	bec_ods.MSC_TRADING_PARTNER_MAPS
where
	(
	NVL(MAP_ID,0) 

	) in 
	(
	select
		NVL(stg.MAP_ID,0) AS MAP_ID 
	from
		bec_ods.MSC_TRADING_PARTNER_MAPS ods,
		bec_ods_stg.MSC_TRADING_PARTNER_MAPS stg
	where
	NVL(ods.MAP_ID,0) = NVL(stg.MAP_ID,0) 
	and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MSC_TRADING_PARTNER_MAPS
    (
	map_id,
	map_type,
	tp_key,
	company_key,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
	map_id,
	map_type,
	tp_key,
	company_key,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.MSC_TRADING_PARTNER_MAPS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(MAP_ID,0), 
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(MAP_ID,0) AS MAP_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.MSC_TRADING_PARTNER_MAPS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(MAP_ID,0) 
			)	
	);

commit;

 
-- Soft delete
update bec_ods.MSC_TRADING_PARTNER_MAPS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MSC_TRADING_PARTNER_MAPS set IS_DELETED_FLG = 'Y'
where (MAP_ID)  in
(
select MAP_ID from bec_raw_dl_ext.MSC_TRADING_PARTNER_MAPS
where (MAP_ID,KCA_SEQ_ID)
in 
(
select MAP_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MSC_TRADING_PARTNER_MAPS
group by MAP_ID
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
	ods_table_name = 'msc_trading_partner_maps';

commit;