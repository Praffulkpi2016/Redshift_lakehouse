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

truncate table bec_ods_stg.jtf_rs_salesreps;

insert
	into
	bec_ods_stg.jtf_rs_salesreps
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
--	is_deleted_flg,
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
	--'N' as IS_DELETED_FLG,
	KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_raw_dl_ext.JTF_RS_SALESREPS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (SALESREP_ID,nvl(ORG_ID,0),kca_seq_id) in 
	(select SALESREP_ID,nvl(ORG_ID,0),max(kca_seq_id) from bec_raw_dl_ext.jtf_rs_salesreps 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by SALESREP_ID,nvl(ORG_ID,0))
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'jtf_rs_salesreps')
			 
            );
end;