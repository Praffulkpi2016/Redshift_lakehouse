 
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
	bec_ods.MSC_COMPANY_SITES
where
	(nvl(COMPANY_ID, 0),
	nvl(COMPANY_SITE_ID, 0) ) in (
	select
		nvl(stg.COMPANY_ID, 0) as COMPANY_ID ,
		nvl(stg.COMPANY_SITE_ID, 0) as COMPANY_SITE_ID 
	from
		bec_ods.MSC_COMPANY_SITES ods,
		bec_ods_stg.MSC_COMPANY_SITES stg
	where
		nvl(ods.COMPANY_ID, 0) = nvl(stg.COMPANY_ID, 0)
			and
nvl(ods.COMPANY_SITE_ID, 0) = nvl(stg.COMPANY_SITE_ID, 0) 
					and
stg.kca_operation in ('INSERT', 'UPDATE') );

commit;
-- Insert records
insert
	into
		bec_ods.MSC_COMPANY_SITES
    ( company_site_id,
		company_id,
		company_site_name,
		sr_instance_id,
		deleted_flag,
		longitude,
		latitude,
		refresh_number,
		disable_date,
		planning_enabled,
		creation_date,
		created_by,
		last_update_date,
		last_updated_by,
		last_update_login,
		attribute_category,
		attribute1,
		attribute2,
		attribute3,
		attribute4,
		attribute5,
		attribute6,
		attribute7,
		attribute8,
		attribute9,
		attribute10,
		attribute11,
		attribute12,
		attribute13,
		attribute14,
		attribute15,
		request_id,
		program_id,
		program_update_date,
		"location",
		address1,
		address2,
		address3,
		address4,
		country,
		state,
		city,
		county,
		province,
		postal_code,
		KCA_OPERATION,
		IS_DELETED_FLG,
		kca_seq_id,
	kca_seq_date)
(
		select
			company_site_id,
			company_id,
			company_site_name,
			sr_instance_id,
			deleted_flag,
			longitude,
			latitude,
			refresh_number,
			disable_date,
			planning_enabled,
			creation_date,
			created_by,
			last_update_date,
			last_updated_by,
			last_update_login,
			attribute_category,
			attribute1,
			attribute2,
			attribute3,
			attribute4,
			attribute5,
			attribute6,
			attribute7,
			attribute8,
			attribute9,
			attribute10,
			attribute11,
			attribute12,
			attribute13,
			attribute14,
			attribute15,
			request_id,
			program_id,
			program_update_date,
			"location",
			address1,
			address2,
			address3,
			address4,
			country,
			state,
			city,
			county,
			province,
			postal_code,
			KCA_OPERATION,
			'N' as IS_DELETED_FLG,
			cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
		from
			bec_ods_stg.MSC_COMPANY_SITES
		where
			kca_operation IN ('INSERT','UPDATE')
			and (nvl(COMPANY_ID, 0),
			nvl(COMPANY_SITE_ID, 0),
			 
			KCA_SEQ_ID) in 
	(
			select
				nvl(COMPANY_ID, 0) as COMPANY_ID ,
				nvl(COMPANY_SITE_ID, 0) as COMPANY_SITE_ID, 
				max(KCA_SEQ_ID)
			from
				bec_ods_stg.MSC_COMPANY_SITES
			where
				kca_operation IN ('INSERT','UPDATE')
			group by
				nvl(COMPANY_ID, 0),
				nvl(COMPANY_SITE_ID, 0) 
				 )	
	);

commit;
 
-- Soft delete
update bec_ods.MSC_COMPANY_SITES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MSC_COMPANY_SITES set IS_DELETED_FLG = 'Y'
where (COMPANY_ID ,COMPANY_SITE_ID)  in
(
select COMPANY_ID ,COMPANY_SITE_ID from bec_raw_dl_ext.MSC_COMPANY_SITES
where (COMPANY_ID ,COMPANY_SITE_ID,KCA_SEQ_ID)
in 
(
select COMPANY_ID ,COMPANY_SITE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MSC_COMPANY_SITES
group by COMPANY_ID ,COMPANY_SITE_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;
update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'msc_company_sites';

commit;  