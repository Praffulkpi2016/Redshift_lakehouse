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
	bec_ods.OE_TRANSACTION_TYPES_TL
where
	(
	nvl(TRANSACTION_TYPE_ID, 0) ,
	nvl(language, 'NA') 
	) in 
	(
	select
		NVL(stg.TRANSACTION_TYPE_ID, 0) as TRANSACTION_TYPE_ID ,
		NVL(stg.LANGUAGE, 'NA') as language
	from
		bec_ods.OE_TRANSACTION_TYPES_TL ods,
		bec_ods_stg.OE_TRANSACTION_TYPES_TL stg
	where
		    NVL(ods.TRANSACTION_TYPE_ID, 0) = NVL(stg.TRANSACTION_TYPE_ID, 0)
			and
			NVL(ods.LANGUAGE, 'NA') = NVL(stg.LANGUAGE, 'NA')
				and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.OE_TRANSACTION_TYPES_TL (
		transaction_type_id,
	"language",
	source_lang,
	"name",
	description,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	request_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date)
(
	select
		transaction_type_id,
		"language",
		source_lang,
		"name",
		description,
		creation_date,
		created_by,
		last_update_date,
		last_updated_by,
		last_update_login,
		program_application_id,
		program_id,
		request_id,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.OE_TRANSACTION_TYPES_TL
	where
		kca_operation in ('INSERT','UPDATE')
		and (
		nvl(TRANSACTION_TYPE_ID, 0) ,
		nvl(language, 'NA'),
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(TRANSACTION_TYPE_ID, 0) as TRANSACTION_TYPE_ID ,
			nvl(language, 'NA') as language,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.OE_TRANSACTION_TYPES_TL
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			TRANSACTION_TYPE_ID,
			language 
			)	
	);

commit;

-- Soft delete
update bec_ods.OE_TRANSACTION_TYPES_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OE_TRANSACTION_TYPES_TL set IS_DELETED_FLG = 'Y'
where (nvl(TRANSACTION_TYPE_ID, 0) ,nvl(language, 'NA'))  in
(
select nvl(TRANSACTION_TYPE_ID, 0) ,nvl(language, 'NA') from bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
where (nvl(TRANSACTION_TYPE_ID, 0) ,nvl(language, 'NA'),KCA_SEQ_ID)
in 
(
select nvl(TRANSACTION_TYPE_ID, 0) ,nvl(language, 'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
group by nvl(TRANSACTION_TYPE_ID, 0) ,nvl(language, 'NA')
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
	ods_table_name = 'oe_transaction_types_tl';

commit;