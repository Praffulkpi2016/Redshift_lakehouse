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

truncate table bec_ods_stg.XLA_TRANSACTION_ENTITIES;
	
	insert into	bec_ods_stg.XLA_TRANSACTION_ENTITIES
(
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
KCA_OPERATION
,kca_seq_id
,KCA_SEQ_DATE
)
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
KCA_OPERATION
,kca_seq_id
,KCA_SEQ_DATE
	from
		bec_raw_dl_ext.xla_transaction_entities

where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' and (ENTITY_ID,kca_seq_id) in (select ENTITY_ID,max(kca_seq_id) from bec_raw_dl_ext.xla_transaction_entities 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
group by ENTITY_ID)
and 
(KCA_SEQ_DATE > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'xla_transaction_entities')
            )

);
end;