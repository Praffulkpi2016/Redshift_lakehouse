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

delete from bec_ods.MSC_COMPANY_RELATIONSHIPS
where RELATIONSHIP_ID in (
select stg.RELATIONSHIP_ID from bec_ods.MSC_COMPANY_RELATIONSHIPS ods, bec_ods_stg.MSC_COMPANY_RELATIONSHIPS stg
where ods.RELATIONSHIP_ID = stg.RELATIONSHIP_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MSC_COMPANY_RELATIONSHIPS
       (
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
        IS_DELETED_FLG,
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.MSC_COMPANY_RELATIONSHIPS
	where kca_operation IN ('INSERT','UPDATE') 
	and (RELATIONSHIP_ID,kca_seq_id) in 
	(select RELATIONSHIP_ID,max(kca_seq_id) from bec_ods_stg.MSC_COMPANY_RELATIONSHIPS 
     where kca_operation IN ('INSERT','UPDATE')
     group by RELATIONSHIP_ID)
);

commit;

 
-- Soft delete
update bec_ods.MSC_COMPANY_RELATIONSHIPS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MSC_COMPANY_RELATIONSHIPS set IS_DELETED_FLG = 'Y'
where (RELATIONSHIP_ID)  in
(
select RELATIONSHIP_ID from bec_raw_dl_ext.MSC_COMPANY_RELATIONSHIPS
where (RELATIONSHIP_ID,KCA_SEQ_ID)
in 
(
select RELATIONSHIP_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MSC_COMPANY_RELATIONSHIPS
group by RELATIONSHIP_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'msc_company_relationships';

commit;