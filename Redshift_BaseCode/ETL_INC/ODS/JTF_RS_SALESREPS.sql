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

delete from bec_ods.jtf_rs_salesreps
where (SALESREP_ID,nvl(ORG_ID,0)) in (
select stg.SALESREP_ID,nvl(stg.ORG_ID,0) from bec_ods.jtf_rs_salesreps ods, bec_ods_stg.jtf_rs_salesreps stg
where ods.SALESREP_ID = stg.SALESREP_ID and nvl(ods.ORG_ID,0) = nvl(stg.ORG_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert
	into
	bec_ods.jtf_rs_salesreps
(	salesrep_id,
	resource_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	sales_credit_type_id,
	"name",
	status,
	start_date_active,
	end_date_active,
	gl_id_rev,
	gl_id_freight,
	gl_id_rec,
	set_of_books_id,
	salesrep_number,
	org_id,
	email_address,
	wh_update_date,
	person_id,
	sales_tax_geocode,
	sales_tax_inside_city_limits,
	object_version_number,
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
	security_group_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	SELECT
	salesrep_id,
	resource_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	sales_credit_type_id,
	"name",
	status,
	start_date_active,
	end_date_active,
	gl_id_rev,
	gl_id_freight,
	gl_id_rec,
	set_of_books_id,
	salesrep_number,
	org_id,
	email_address,
	wh_update_date,
	person_id,
	sales_tax_geocode,
	sales_tax_inside_city_limits,
	object_version_number,
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
	security_group_id,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.jtf_rs_salesreps
	where kca_operation IN ('INSERT','UPDATE') 
	and (SALESREP_ID,nvl(ORG_ID,0),kca_seq_id) in 
	(select SALESREP_ID,nvl(ORG_ID,0),max(kca_seq_id) from bec_ods_stg.jtf_rs_salesreps 
     where kca_operation IN ('INSERT','UPDATE')
     group by SALESREP_ID,nvl(ORG_ID,0));

commit;

 
-- Soft delete
update bec_ods.jtf_rs_salesreps set IS_DELETED_FLG = 'N';
commit;
update bec_ods.jtf_rs_salesreps set IS_DELETED_FLG = 'Y'
where (SALESREP_ID,nvl(ORG_ID,0))  in
(
select SALESREP_ID,nvl(ORG_ID,0) from bec_raw_dl_ext.jtf_rs_salesreps
where (SALESREP_ID,nvl(ORG_ID,0),KCA_SEQ_ID)
in 
(
select SALESREP_ID,nvl(ORG_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.jtf_rs_salesreps
group by SALESREP_ID,nvl(ORG_ID,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'jtf_rs_salesreps';

commit;