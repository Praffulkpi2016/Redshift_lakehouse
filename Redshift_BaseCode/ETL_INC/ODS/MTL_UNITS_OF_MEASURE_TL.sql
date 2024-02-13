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
	bec_ods.MTL_UNITS_OF_MEASURE_TL
where
	(nvl(UNIT_OF_MEASURE, 'NA'),
	nvl(language, 'NA')) in (
	select
		nvl(stg.UNIT_OF_MEASURE, 'NA') as UNIT_OF_MEASURE,
		nvl(stg.LANGUAGE, 'NA') as language
	from
		bec_ods.MTL_UNITS_OF_MEASURE_TL ods,
		bec_ods_stg.MTL_UNITS_OF_MEASURE_TL stg
	where
		nvl(ods.UNIT_OF_MEASURE, 'NA') = nvl(stg.UNIT_OF_MEASURE, 'NA')
			and nvl(ods.LANGUAGE, 'NA') = nvl(stg.LANGUAGE, 'NA')
				and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MTL_UNITS_OF_MEASURE_TL
       (
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
	kca_operation,
	is_deleted_flg,
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.MTL_UNITS_OF_MEASURE_TL
	where
		kca_operation in ('INSERT','UPDATE')
		and (nvl(UNIT_OF_MEASURE, 'NA'),
		nvl(language, 'NA'),
		kca_seq_id) in 
	(
		select
			nvl(UNIT_OF_MEASURE, 'NA') as UNIT_OF_MEASURE,
			nvl(language, 'NA') as language,
			max(kca_seq_id)
		from
			bec_ods_stg.MTL_UNITS_OF_MEASURE_TL
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			nvl(UNIT_OF_MEASURE, 'NA'),
			nvl(language, 'NA'))
);

commit;

-- Soft delete
update bec_ods.MTL_UNITS_OF_MEASURE_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_UNITS_OF_MEASURE_TL set IS_DELETED_FLG = 'Y'
where (UNIT_OF_MEASURE,language)  in
(
select UNIT_OF_MEASURE,language from bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
where (UNIT_OF_MEASURE,language,KCA_SEQ_ID)
in 
(
select UNIT_OF_MEASURE,language,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
group by UNIT_OF_MEASURE,language
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
	ods_table_name = 'mtl_units_of_measure_tl';

commit;