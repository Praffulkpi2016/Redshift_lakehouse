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
	table bec_ods_stg.FA_LOCATIONS;

insert
	into
	bec_ods_stg.FA_LOCATIONS
    (location_id,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
	last_update_date,
	last_updated_by,
	last_update_login,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	attribute_category_code,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		location_id,
		segment1,
		segment2,
		segment3,
		segment4,
		segment5,
		segment6,
		segment7,
		summary_flag,
		enabled_flag,
		start_date_active,
		end_date_active,
		last_update_date,
		last_updated_by,
		last_update_login,
		attribute1,
		attribute2,
		attribute3,
		attribute4,
		attribute5,
		attribute6,
		attribute7,
		attribute8,
		attribute9,
		attribute10,
		attribute11,
		attribute12,
		attribute13,
		attribute14,
		attribute15,
		attribute_category_code,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.FA_LOCATIONS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(LOCATION_ID, 0),
		KCA_SEQ_ID) in 
	(
		select
			nvl(LOCATION_ID, 0) as LOCATION_ID,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.FA_LOCATIONS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(LOCATION_ID, 0))
		and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_locations')
);
end;