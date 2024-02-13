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
	table bec_ods_stg.MTL_UNITS_OF_MEASURE_TL;

insert
	into
	bec_ods_stg.MTL_UNITS_OF_MEASURE_TL
( unit_of_measure,
	uom_code,
	uom_class,
	base_uom_flag,
	unit_of_measure_tl,
	last_update_date,
	last_updated_by,
	created_by,
	creation_date,
	last_update_login,
	disable_date,
	description,
	"language",
	source_lang,
	attribute_category,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	zd_edition_name,
	zd_sync,
	KCA_OPERATION,
	kca_seq_id
	,kca_seq_date
)
(
	select
		unit_of_measure,
		uom_code,
		uom_class,
		base_uom_flag,
		unit_of_measure_tl,
		last_update_date,
		last_updated_by,
		created_by,
		creation_date,
		last_update_login,
		disable_date,
		description,
		"language",
		source_lang,
		attribute_category,
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
		request_id,
		program_application_id,
		program_id,
		program_update_date,
		zd_edition_name,
		zd_sync,
		KCA_OPERATION,
		kca_seq_id
		,kca_seq_date
	from
		bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
	where
		kca_operation != 'DELETE' and nvl(kca_seq_id,'') != ''
		and (nvl(UNIT_OF_MEASURE, 'NA'),
		nvl(language, 'NA'),
		KCA_SEQ_ID) in 
(
		select
			nvl(UNIT_OF_MEASURE, 'NA') as UNIT_OF_MEASURE,
			nvl(language, 'NA') as language,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
		where
			kca_operation != 'DELETE' and nvl(kca_seq_id,'') != ''
		group by
			nvl(UNIT_OF_MEASURE, 'NA'),
			nvl(language, 'NA'))
		and (kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'mtl_units_of_measure_tl')

            )	
			);
end;