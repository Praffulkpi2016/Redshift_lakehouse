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

truncate table bec_ods_stg.fnd_document_sequences;

insert into	bec_ods_stg.fnd_document_sequences
   (DOC_SEQUENCE_ID,
	NAME,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	APPLICATION_ID,
	AUDIT_TABLE_NAME,
	MESSAGE_FLAG,
	START_DATE,
	TYPE,
	DB_SEQUENCE_NAME,
	END_DATE,
	INITIAL_VALUE,
	TABLE_NAME,
	ATTRIBUTE_CATEGORY,
	ATTRIBUTE1,
	ATTRIBUTE2,
	ATTRIBUTE3,
	ATTRIBUTE4,
	ATTRIBUTE5,
	ATTRIBUTE6,
	ATTRIBUTE7,
	ATTRIBUTE8,
	ATTRIBUTE9,
	ATTRIBUTE10,
	ATTRIBUTE11,
	ATTRIBUTE12,
	ATTRIBUTE13,
	ATTRIBUTE14,
	ATTRIBUTE15,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		DOC_SEQUENCE_ID,
		NAME,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		APPLICATION_ID,
		AUDIT_TABLE_NAME,
		MESSAGE_FLAG,
		START_DATE,
		TYPE,
		DB_SEQUENCE_NAME,
		END_DATE,
		INITIAL_VALUE,
		TABLE_NAME,
		ATTRIBUTE_CATEGORY,
		ATTRIBUTE1,
		ATTRIBUTE2,
		ATTRIBUTE3,
		ATTRIBUTE4,
		ATTRIBUTE5,
		ATTRIBUTE6,
		ATTRIBUTE7,
		ATTRIBUTE8,
		ATTRIBUTE9,
		ATTRIBUTE10,
		ATTRIBUTE11,
		ATTRIBUTE12,
		ATTRIBUTE13,
		ATTRIBUTE14,
		ATTRIBUTE15,
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.fnd_document_sequences
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (DOC_SEQUENCE_ID,kca_seq_id) in 
	(select DOC_SEQUENCE_ID,max(kca_seq_id) from bec_raw_dl_ext.fnd_document_sequences 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by DOC_SEQUENCE_ID)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fnd_document_sequences')
);
end;