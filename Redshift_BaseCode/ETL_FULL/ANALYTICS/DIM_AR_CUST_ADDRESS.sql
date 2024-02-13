/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_dwh.DIM_AR_CUST_ADDRESS;

create table bec_dwh.dim_ar_cust_address diststyle all sortkey(customer_trx_id)
as
 select
	distinct 
    CT.customer_trx_id,
	ct.trx_number,
	hca_shipto.party_id,
	ct.bill_to_site_use_id,
	ct.ship_to_site_use_id,
	ct.bill_to_customer_id,
	ct.ship_to_customer_id,
	bhcsua_shipto.cust_acct_site_id as bill_to_cust_acct_site_id,
	hcsua_shipto.cust_acct_site_id as ship_to_cust_acct_site_id,
	bhcasa_shipto.party_site_id as bill_to_party_site_id,
	hcasa_shipto.party_site_id as ship_to_party_site_id,
	bhps.location_id as bill_to_location_id,
	hps.location_id as ship_to_location_id,
	hca_shipto.account_number ship_to_account ,
    hz_shipto.party_name ship_to_customer_name ,
	hl.address1 ship_to_address1 ,
	hl.address2 ship_to_address2 ,
	hl.address3 ship_to_address3 ,
	hl.country ship_to_country ,
	hl.state ship_state ,
	hl.city ship_city ,
	hl.postal_code ship_zip ,
	hca_billto.account_number bill_to_account ,
	hz_billto.party_name bill_to_customer_name ,
	Bhl.address1 Bill_to_address1 ,
	Bhl.address2 Bill_to_address2 ,
	Bhl.address3 Bill_to_address3 ,
	Bhl.country Bill_to_country ,
	Bhl.state Bill_State ,
	Bhl.city Bill_City ,
	Bhl.postal_code Bill_Zip,
	nvl(hz_shipto.party_name, hz_billto.party_name) customer_name ,
    nvl(hz_shipto.party_number, hz_billto.party_number) customer_number ,
    nvl(hca_shipto.account_number ,    hca_billto.account_number)  customer_account_number ,
			'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || nvl(ct.customer_trx_id, 0) 
		||'-'||nvl(ct.bill_to_customer_id,0)
		||'-'||nvl(ct.ship_to_customer_id,0) 
		||'-'||nvl(hca_shipto.party_id,0)
		 as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.ra_customer_trx_all ct ,
	bec_ods.hz_cust_accounts hca_billto ,
	bec_ods.hz_parties hz_billto ,
	bec_ods.hz_cust_accounts hca_shipto ,
	bec_ods.hz_cust_acct_sites_all hcasa_shipto ,
	bec_ods.hz_cust_site_uses_all hcsua_shipto ,
	bec_ods.hz_parties hz_shipto ,
	bec_ods.hz_cust_accounts lhca_shipto ,	
	bec_ods.hz_party_sites hps ,
	bec_ods.hz_locations hl,
	----Bill To 
	bec_ods.hz_cust_acct_sites_all bhcasa_shipto ,
	bec_ods.hz_cust_site_uses_all bhcsua_shipto ,
	bec_ods.hz_party_sites bhps ,
	bec_ods.hz_locations bhl
where
	1 = 1
	and ct.bill_to_customer_id = hca_billto.cust_account_id
	and hca_billto.party_id = hz_billto.party_id
	and ct.ship_to_customer_id = hca_shipto.cust_account_id (+)
	and hca_shipto.party_id = hz_shipto.party_id (+)
	and ct.ship_to_site_use_id = hcsua_shipto.site_use_id (+)
	and hcsua_shipto.SITE_USE_CODE(+) = 'SHIP_TO'
	and hcsua_shipto.cust_acct_site_id = hcasa_shipto.cust_acct_site_id (+)
	and hcasa_shipto.party_site_id = hps.party_site_id (+)
	and hps.location_id = hl.location_id (+)
	and ct.bill_to_site_use_id = bhcsua_shipto.site_use_id (+)
	and Bhcsua_shipto.SITE_USE_CODE(+) = 'BILL_TO'
	and bhcsua_shipto.cust_acct_site_id = bhcasa_shipto.cust_acct_site_id (+)
	and bhcasa_shipto.party_site_id = bhps.party_site_id (+)
	and bhps.location_id = bhl.location_id (+)
;
end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ar_cust_address'
	and batch_name = 'ar';

commit;