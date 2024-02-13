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
	bec_ods.FA_ASSET_HISTORY
where
	(
	nvl(ASSET_ID, 0),
	nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00')
	) in 
	(
	select
		NVL(stg.ASSET_ID, 0) as ASSET_ID,
		nvl(stg.DATE_EFFECTIVE, '1900-01-01 00:00:00') as DATE_EFFECTIVE
	from
		bec_ods.FA_ASSET_HISTORY ods,
		bec_ods_stg.FA_ASSET_HISTORY stg
	where
		NVL(ods.ASSET_ID, 0) = NVL(stg.ASSET_ID, 0)
			and nvl(ods.DATE_EFFECTIVE, '1900-01-01 00:00:00') = nvl(stg.DATE_EFFECTIVE, '1900-01-01 00:00:00')
				and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.FA_ASSET_HISTORY (
	asset_id,
	category_id,
	asset_type,
	units,
	date_effective,
	date_ineffective,
	transaction_header_id_in,
	transaction_header_id_out,
	last_update_date,
	last_updated_by,
	last_update_login,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
		asset_id,
		category_id,
		asset_type,
		units,
		date_effective,
		date_ineffective,
		transaction_header_id_in,
		transaction_header_id_out,
		last_update_date,
		last_updated_by,
		last_update_login,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.FA_ASSET_HISTORY
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		nvl(ASSET_ID, 0),
		nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00'),
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(ASSET_ID, 0) as ASSET_ID,
			nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00') as DATE_EFFECTIVE,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.FA_ASSET_HISTORY
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			nvl(ASSET_ID, 0),
		nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00') 
			)	
	);

commit;

-- Soft delete
update bec_ods.FA_ASSET_HISTORY set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_ASSET_HISTORY set IS_DELETED_FLG = 'Y'
where (nvl(ASSET_ID, 0),nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00'))  in
(
select nvl(ASSET_ID, 0),nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00') from bec_raw_dl_ext.FA_ASSET_HISTORY
where (nvl(ASSET_ID, 0),nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00'),KCA_SEQ_ID)
in 
(
select nvl(ASSET_ID, 0),nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_ASSET_HISTORY
group by nvl(ASSET_ID, 0),nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00')
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
	ods_table_name = 'fa_asset_history';

commit;