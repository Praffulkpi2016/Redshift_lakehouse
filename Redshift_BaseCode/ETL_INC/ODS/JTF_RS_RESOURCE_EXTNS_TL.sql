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

delete from bec_ods.jtf_rs_resource_extns_tl
where (RESOURCE_ID,CATEGORY,LANGUAGE) in (
select stg.RESOURCE_ID,stg.CATEGORY,stg.LANGUAGE from bec_ods.jtf_rs_resource_extns_tl ods, bec_ods_stg.jtf_rs_resource_extns_tl stg
where ods.RESOURCE_ID = stg.RESOURCE_ID and stg.CATEGORY = ods.CATEGORY and stg.LANGUAGE = ods.LANGUAGE and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert
	into
	bec_ods.jtf_rs_resource_extns_tl
(created_by,
	resource_id,
	category,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	"language",
	resource_name,
	source_lang,
	security_group_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	SELECT
	created_by,
	resource_id,
	category,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	"language",
	resource_name,
	source_lang,
	security_group_id,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.jtf_rs_resource_extns_tl
	where kca_operation IN ('INSERT','UPDATE') 
	and (RESOURCE_ID,CATEGORY,LANGUAGE,kca_seq_id) in 
	(select RESOURCE_ID,CATEGORY,LANGUAGE,max(kca_seq_id) from bec_ods_stg.jtf_rs_resource_extns_tl 
     where kca_operation IN ('INSERT','UPDATE')
     group by RESOURCE_ID,CATEGORY,LANGUAGE);

commit;

 
-- Soft delete
update bec_ods.jtf_rs_resource_extns_tl set IS_DELETED_FLG = 'N';
commit;
update bec_ods.jtf_rs_resource_extns_tl set IS_DELETED_FLG = 'Y'
where (RESOURCE_ID,CATEGORY,LANGUAGE)  in
(
select RESOURCE_ID,CATEGORY,LANGUAGE from bec_raw_dl_ext.jtf_rs_resource_extns_tl
where (RESOURCE_ID,CATEGORY,LANGUAGE,KCA_SEQ_ID)
in 
(
select RESOURCE_ID,CATEGORY,LANGUAGE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.jtf_rs_resource_extns_tl
group by RESOURCE_ID,CATEGORY,LANGUAGE
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'jtf_rs_resource_extns_tl';

commit;