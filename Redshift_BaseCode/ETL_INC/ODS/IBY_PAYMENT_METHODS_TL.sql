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

delete from bec_ods.IBY_PAYMENT_METHODS_TL
where (nvl(PAYMENT_METHOD_CODE,'NA'),nvl(LANGUAGE,'NA'))  in (
select nvl(stg.PAYMENT_METHOD_CODE,'NA') as PAYMENT_METHOD_CODE,nvl(stg.LANGUAGE,'NA') as LANGUAGE from bec_ods.IBY_PAYMENT_METHODS_TL ods, bec_ods_stg.IBY_PAYMENT_METHODS_TL stg
where nvl(ods.PAYMENT_METHOD_CODE,'NA') = nvl(stg.PAYMENT_METHOD_CODE,'NA')
and nvl(ods.LANGUAGE,'NA') = nvl(stg.LANGUAGE,'NA')
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.IBY_PAYMENT_METHODS_TL
       (	payment_method_code,
	"language",
	source_lang,
	payment_method_name,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	object_version_number,
	description,
	zd_edition_name,
	zd_sync,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
			payment_method_code,
	"language",
	source_lang,
	payment_method_name,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	object_version_number,
	description,
	zd_edition_name,
	zd_sync,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.IBY_PAYMENT_METHODS_TL
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(PAYMENT_METHOD_CODE,'NA'),nvl(LANGUAGE,'NA'),kca_seq_id) in 
	(select nvl(PAYMENT_METHOD_CODE,'NA') as PAYMENT_METHOD_CODE,nvl(LANGUAGE,'NA') as LANGUAGE,max(kca_seq_id) from bec_ods_stg.IBY_PAYMENT_METHODS_TL 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(PAYMENT_METHOD_CODE,'NA'),nvl(LANGUAGE,'NA' ))
);

commit;

-- Soft delete
update bec_ods.IBY_PAYMENT_METHODS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.IBY_PAYMENT_METHODS_TL set IS_DELETED_FLG = 'Y'
where (nvl(PAYMENT_METHOD_CODE,'NA'),nvl(LANGUAGE,'NA' ))  in
(
select nvl(PAYMENT_METHOD_CODE,'NA'),nvl(LANGUAGE,'NA' ) from bec_raw_dl_ext.IBY_PAYMENT_METHODS_TL
where (nvl(PAYMENT_METHOD_CODE,'NA'),nvl(LANGUAGE,'NA' ),KCA_SEQ_ID)
in 
(
select nvl(PAYMENT_METHOD_CODE,'NA'),nvl(LANGUAGE,'NA' ),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.IBY_PAYMENT_METHODS_TL
group by nvl(PAYMENT_METHOD_CODE,'NA'),nvl(LANGUAGE,'NA' )
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'iby_payment_methods_tl';

commit;