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

truncate table bec_ods_stg.MSC_COMPANY_RELATIONSHIPS;

insert into	bec_ods_stg.MSC_COMPANY_RELATIONSHIPS
   (RELATIONSHIP_ID,
	SUBJECT_ID,
	OBJECT_ID,
	RELATIONSHIP_TYPE,
	START_DATE,
	END_DATE,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		RELATIONSHIP_ID,
		SUBJECT_ID,
		OBJECT_ID,
		RELATIONSHIP_TYPE,
		START_DATE,
		END_DATE,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
        KCA_OPERATION,
		kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.MSC_COMPANY_RELATIONSHIPS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (RELATIONSHIP_ID,kca_seq_id) in 
	(select RELATIONSHIP_ID,max(kca_seq_id) from bec_raw_dl_ext.MSC_COMPANY_RELATIONSHIPS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by RELATIONSHIP_ID)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'msc_company_relationships')
			 
            )
);
end;