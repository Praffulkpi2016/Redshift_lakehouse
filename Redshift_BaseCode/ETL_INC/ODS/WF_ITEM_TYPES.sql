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

delete from bec_ods.wf_item_types
where (nvl(NAME,'NA')) in (
select nvl(stg.NAME,'NA') as NAME
from bec_ods.wf_item_types ods,  bec_ods_stg.wf_item_types stg
where nvl(ods.NAME,'NA') = nvl(stg.NAME,'NA') 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

INSERT INTO bec_ods.wf_item_types
(
	NAME,
	PROTECT_LEVEL,
	CUSTOM_LEVEL,
	WF_SELECTOR,
	READ_ROLE,
	WRITE_ROLE,
	EXECUTE_ROLE,
	PERSISTENCE_TYPE,
	PERSISTENCE_DAYS,
	SECURITY_GROUP_ID,
	NUM_ACTIVE,
	NUM_ERROR,
	NUM_DEFER,
	NUM_SUSPEND,
	NUM_COMPLETE,
	NUM_PURGEABLE,
	ZD_EDITION_NAME,
	ZD_SYNC,
	KCA_OPERATION,
	IS_DELETED_FLG,
	KCA_SEQ_ID
	,kca_seq_date
)
(SELECT
	NAME,
	PROTECT_LEVEL,
	CUSTOM_LEVEL,
	WF_SELECTOR,
	READ_ROLE,
	WRITE_ROLE,
	EXECUTE_ROLE,
	PERSISTENCE_TYPE,
	PERSISTENCE_DAYS,
	SECURITY_GROUP_ID,
	NUM_ACTIVE,
	NUM_ERROR,
	NUM_DEFER,
	NUM_SUSPEND,
	NUM_COMPLETE,
	NUM_PURGEABLE,
	ZD_EDITION_NAME,
	ZD_SYNC,
	KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.wf_item_types
	where kca_operation in ('INSERT','UPDATE') 
	and (nvl(NAME,'NA'),kca_seq_id) in 
	(select nvl(NAME,'NA') as NAME,max(kca_seq_id) from bec_ods_stg.wf_item_types 
     where kca_operation in ('INSERT','UPDATE')
     group by nvl(NAME,'NA'))
);

commit;


-- Soft delete
update bec_ods.wf_item_types set IS_DELETED_FLG = 'N';
commit;
update bec_ods.wf_item_types set IS_DELETED_FLG = 'Y'
where (NAME)  in
(
select NAME from bec_raw_dl_ext.wf_item_types
where (NAME,KCA_SEQ_ID)
in 
(
select NAME,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.wf_item_types
group by NAME
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate(),load_type = 'I'
where ods_table_name = 'wf_item_types';

commit;