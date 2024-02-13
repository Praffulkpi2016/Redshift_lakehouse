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
	table bec_ods_stg.OE_TRANSACTION_TYPES_TL;

insert
	into
	bec_ods_stg.OE_TRANSACTION_TYPES_TL
    (transaction_type_id,
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
	KCA_OPERATION,
	KCA_SEQ_ID
	,KCA_SEQ_DATE)
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
		KCA_OPERATION,
		KCA_SEQ_ID
		,KCA_SEQ_DATE
	from
		bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
	where
		kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
		and (nvl(TRANSACTION_TYPE_ID, 0) ,
		nvl(language, 'NA'),
		KCA_SEQ_ID) in 
	(
		select
			nvl(TRANSACTION_TYPE_ID, 0) as TRANSACTION_TYPE_ID ,
			nvl(language, 'NA') as language,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.OE_TRANSACTION_TYPES_TL
		where
			kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
		group by
			TRANSACTION_TYPE_ID,
			language )
		and (KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'oe_transaction_types_tl')
)
);
end;