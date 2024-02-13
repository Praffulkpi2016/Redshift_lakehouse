/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records

delete from bec_dwh.DIM_AR_CUST_ADDRESS
where (nvl(customer_trx_id, 0) ,nvl(bill_to_customer_id,0),nvl(ship_to_customer_id,0) ,nvl(party_id,0)) in (
select nvl(ods.customer_trx_id, 0) 
,nvl(ods.bill_to_customer_id,0)
,nvl(ods.ship_to_customer_id,0) 
,nvl(ods.party_id,0) 
from bec_dwh.DIM_AR_CUST_ADDRESS dw,
 ( select
	distinct 
    CT.customer_trx_id,
	hca_shipto.party_id,
	ct.bill_to_customer_id,
	ct.ship_to_customer_id
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
	AND (ct.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name  ='dim_ar_cust_address' and batch_name = 'ar') OR 
	hca_shipto.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name  ='dim_ar_cust_address' and batch_name = 'ar') )
	) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-' || nvl(ods.customer_trx_id, 0) 
		||'-'||nvl(ods.bill_to_customer_id,0)
		||'-'||nvl(ods.ship_to_customer_id,0) 
		||'-'||nvl(ods.party_id,0)		 
);

commit;

-- Insert records

insert into bec_dwh.DIM_AR_CUST_ADDRESS
(
	customer_trx_id,
	trx_number,
	party_id,
	bill_to_site_use_id,
	ship_to_site_use_id,
	bill_to_customer_id,
	ship_to_customer_id,
	bill_to_cust_acct_site_id,
	ship_to_cust_acct_site_id,
	bill_to_party_site_id,
	ship_to_party_site_id,
	bill_to_location_id,
	ship_to_location_id,
	ship_to_account,
	ship_to_customer_name,
	ship_to_address1,
	ship_to_address2,
	ship_to_address3,
	ship_to_country,
	ship_state,
	ship_city,
	ship_zip,
	bill_to_account,
	bill_to_customer_name,
	bill_to_address1,
	bill_to_address2,
	bill_to_address3,
	bill_to_country,
	bill_state,
	bill_city,
	bill_zip,
	customer_name,
	customer_number,
	customer_account_number,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
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
	AND (ct.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name  ='dim_ar_cust_address' and batch_name = 'ar') OR 
	hca_shipto.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name  ='dim_ar_cust_address' and batch_name = 'ar')
	 )
	);

-- Soft delete

update bec_dwh.DIM_AR_CUST_ADDRESS set is_deleted_flg = 'Y'
where (nvl(customer_trx_id, 0) ,nvl(bill_to_customer_id,0),nvl(ship_to_customer_id,0),nvl(party_id,0)) not in (
select nvl(ods.customer_trx_id, 0) 
,nvl(ods.bill_to_customer_id,0)
,nvl(ods.ship_to_customer_id,0) 
,nvl(ods.party_id,0) from bec_dwh.DIM_AR_CUST_ADDRESS dw,
 (
 select
	distinct 
    CT.customer_trx_id,
	hca_shipto.party_id,
	ct.bill_to_customer_id,
	ct.ship_to_customer_id
from
	(select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y')ct ,
	(select * from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y') hca_billto ,
	(select * from bec_ods.hz_parties where is_deleted_flg <> 'Y') hz_billto ,
	(select * from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y') hca_shipto ,
	(select * from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y') hcasa_shipto ,
	(select * from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y') hcsua_shipto ,
	(select * from bec_ods.hz_parties where is_deleted_flg <> 'Y') hz_shipto ,
	(select * from bec_ods.hz_cust_accounts where is_deleted_flg <> 'Y') lhca_shipto ,	
	(select * from bec_ods.hz_party_sites where is_deleted_flg <> 'Y') hps ,
	(select * from bec_ods.hz_locations where is_deleted_flg <> 'Y') hl,
	----Bill To 
	(select * from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y') bhcasa_shipto ,
	(select * from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y') bhcsua_shipto ,
	(select * from bec_ods.hz_party_sites where is_deleted_flg <> 'Y') bhps ,
	(select * from bec_ods.hz_locations  where is_deleted_flg <> 'Y')bhl
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
) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')|| '-' || nvl(ods.customer_trx_id, 0) 
		||'-'||nvl(ods.bill_to_customer_id,0)
		||'-'||nvl(ods.ship_to_customer_id,0) 
		||'-'||nvl(ods.party_id,0)		 
);

commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_ar_cust_address'
	and batch_name = 'ar';

COMMIT;