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
BEGIN;

TRUNCATE TABLE bec_ods_stg.MSC_TRADING_PARTNER_SITES ;

insert into	bec_ods_stg.MSC_TRADING_PARTNER_SITES 
    (  partner_site_id,
	sr_tp_site_id,
	sr_instance_id,
	partner_id,
	partner_address,
	tp_site_code,
	sr_tp_id,
	"location",
	partner_type,
	deleted_flag,
	longitude,
	latitude,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	operating_unit_name,
	country,
	state,
	city,
	postal_code,
	shipping_control,
	KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date)
(select
	 partner_site_id,
	sr_tp_site_id,
	sr_instance_id,
	partner_id,
	partner_address,
	tp_site_code,
	sr_tp_id,
	"location",
	partner_type,
	deleted_flag,
	longitude,
	latitude,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	operating_unit_name,
	country,
	state,
	city,
	postal_code,
	shipping_control,
	KCA_OPERATION,
	KCA_SEQ_ID,
	kca_seq_date
from
	bec_raw_dl_ext.MSC_TRADING_PARTNER_SITES  
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (nvl(PARTNER_SITE_ID,0),KCA_SEQ_ID) in 
	(select nvl(PARTNER_SITE_ID,0) as PARTNER_SITE_ID ,max(KCA_SEQ_ID) from bec_raw_dl_ext.MSC_TRADING_PARTNER_SITES  
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by nvl(PARTNER_SITE_ID,0))
     and ( kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info 
	 where ods_table_name ='msc_trading_partner_sites')
	 
            )
	 
	 );
END;