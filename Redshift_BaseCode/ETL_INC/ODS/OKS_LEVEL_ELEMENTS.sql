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

delete from bec_ods.OKS_LEVEL_ELEMENTS
where (nvl(ID,'NA')) in (
select nvl(stg.ID,'NA') as ID from bec_ods.OKS_LEVEL_ELEMENTS ods, bec_ods_stg.OKS_LEVEL_ELEMENTS stg
where nvl(ods.ID,'NA') = nvl(stg.ID,'NA')  
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.OKS_LEVEL_ELEMENTS
       (	
		ID,
		SEQUENCE_NUMBER,
		DATE_START,
		AMOUNT,
		DATE_RECEIVABLE_GL,
		DATE_REVENUE_RULE_START,
		DATE_TRANSACTION,
		DATE_DUE,
		DATE_PRINT,
		DATE_TO_INTERFACE,
		DATE_COMPLETED,
		OBJECT_VERSION_NUMBER,
		RUL_ID,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		SECURITY_GROUP_ID,
		CLE_ID,
		DNZ_CHR_ID,
		PARENT_CLE_ID,
		DATE_END,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		ID,
		SEQUENCE_NUMBER,
		DATE_START,
		AMOUNT,
		DATE_RECEIVABLE_GL,
		DATE_REVENUE_RULE_START,
		DATE_TRANSACTION,
		DATE_DUE,
		DATE_PRINT,
		DATE_TO_INTERFACE,
		DATE_COMPLETED,
		OBJECT_VERSION_NUMBER,
		RUL_ID,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		SECURITY_GROUP_ID,
		CLE_ID,
		DNZ_CHR_ID,
		PARENT_CLE_ID,
		DATE_END,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.OKS_LEVEL_ELEMENTS
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(ID,'NA'),kca_seq_id) in 
	(select nvl(ID,'NA') as ID,max(kca_seq_id) as kca_seq_id from bec_ods_stg.OKS_LEVEL_ELEMENTS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(ID,'NA')
	 )
);

commit;

-- Soft delete
update bec_ods.OKS_LEVEL_ELEMENTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.OKS_LEVEL_ELEMENTS set IS_DELETED_FLG = 'Y'
where (ID)  in
(
select ID from bec_raw_dl_ext.OKS_LEVEL_ELEMENTS
where (ID,KCA_SEQ_ID)
in 
(
select ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.OKS_LEVEL_ELEMENTS
group by ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'oks_level_elements';

commit;