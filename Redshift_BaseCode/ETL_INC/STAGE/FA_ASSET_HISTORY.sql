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
	table bec_ods_stg.FA_ASSET_HISTORY;

insert
	into
	bec_ods_stg.FA_ASSET_HISTORY
    (asset_id,
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
	KCA_OPERATION,
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
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.FA_ASSET_HISTORY
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(ASSET_ID, 0),
		nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00'),
		KCA_SEQ_ID) in 
	(
		select
			nvl(ASSET_ID, 0) as ASSET_ID,
			nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00') as DATE_EFFECTIVE,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.FA_ASSET_HISTORY
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(ASSET_ID, 0),
		nvl(DATE_EFFECTIVE, '1900-01-01 00:00:00'))
		and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_asset_history')
);
end;