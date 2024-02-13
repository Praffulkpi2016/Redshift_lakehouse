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
	bec_ods.xla_transaction_entities
where
	ENTITY_ID in (
	select
		stg.ENTITY_ID
	from
		bec_ods.xla_transaction_entities ods,
		bec_ods_stg.xla_transaction_entities stg
	where
		ods.ENTITY_ID = stg.ENTITY_ID
		and stg.kca_operation in ('INSERT', 'UPDATE'));

commit;
-- Insert records

insert
	into
	bec_ods.xla_transaction_entities
(ENTITY_ID
,
	APPLICATION_ID
,
	LEGAL_ENTITY_ID
,
	ENTITY_CODE
,
	CREATION_DATE
,
	CREATED_BY
,
	LAST_UPDATE_DATE
,
	LAST_UPDATED_BY
,
	LAST_UPDATE_LOGIN
,
	SOURCE_ID_INT_1
,
	SOURCE_ID_CHAR_1
,
	SECURITY_ID_INT_1
,
	SECURITY_ID_INT_2
,
	SECURITY_ID_INT_3
,
	SECURITY_ID_CHAR_1
,
	SECURITY_ID_CHAR_2
,
	SECURITY_ID_CHAR_3
,
	SOURCE_ID_INT_2
,
	SOURCE_ID_CHAR_2
,
	SOURCE_ID_INT_3
,
	SOURCE_ID_CHAR_3
,
	SOURCE_ID_INT_4
,
	SOURCE_ID_CHAR_4
,
	TRANSACTION_NUMBER
,
	LEDGER_ID
,
	VALUATION_METHOD
,
	SOURCE_APPLICATION_ID
,
	UPG_BATCH_ID
,
	UPG_SOURCE_APPLICATION_ID
,
	UPG_VALID_FLAG
,
	KCA_OPERATION
,
	IS_DELETED_FLG
,
	KCA_SEQ_ID
	,kca_seq_date)
(
	select
		ENTITY_ID,
		APPLICATION_ID,
		LEGAL_ENTITY_ID,
		ENTITY_CODE,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		SOURCE_ID_INT_1,
		SOURCE_ID_CHAR_1,
		SECURITY_ID_INT_1,
		SECURITY_ID_INT_2,
		SECURITY_ID_INT_3,
		SECURITY_ID_CHAR_1,
		SECURITY_ID_CHAR_2,
		SECURITY_ID_CHAR_3,
		SOURCE_ID_INT_2,
		SOURCE_ID_CHAR_2,
		SOURCE_ID_INT_3,
		SOURCE_ID_CHAR_3,
		SOURCE_ID_INT_4,
		SOURCE_ID_CHAR_4,
		TRANSACTION_NUMBER,
		LEDGER_ID,
		VALUATION_METHOD,
		SOURCE_APPLICATION_ID,
		UPG_BATCH_ID,
		UPG_SOURCE_APPLICATION_ID,
		UPG_VALID_FLAG,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.xla_transaction_entities
	where
		kca_operation in ('INSERT','UPDATE')
		and (ENTITY_ID,
		kca_seq_id) in (
		select
			ENTITY_ID,
			max(kca_seq_id)
		from
			bec_ods_stg.xla_transaction_entities
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			ENTITY_ID)
);

commit;

-- Soft delete
update bec_ods.xla_transaction_entities set IS_DELETED_FLG = 'N';
commit;
update bec_ods.xla_transaction_entities set IS_DELETED_FLG = 'Y'
where (ENTITY_ID)  in
(
select ENTITY_ID from bec_raw_dl_ext.xla_transaction_entities
where (ENTITY_ID,KCA_SEQ_ID)
in 
(
select ENTITY_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.xla_transaction_entities
group by ENTITY_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'xla_transaction_entities';

commit;