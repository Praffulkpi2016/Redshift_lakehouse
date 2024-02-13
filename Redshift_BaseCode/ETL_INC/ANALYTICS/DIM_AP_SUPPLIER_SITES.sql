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

delete from bec_dwh.dim_ap_supplier_sites
where nvl(vendor_site_id,0) in (
select nvl(ods.vendor_site_id,0) from bec_dwh.dim_ap_supplier_sites dw, bec_ods.ap_supplier_sites_all ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.vendor_site_id,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_supplier_sites' and batch_name = 'ap'))
);

commit;

-- Insert records

INSERT INTO bec_dwh.DIM_AP_SUPPLIER_SITES
(
vendor_site_id
,vendor_site_code
,vendor_id
,purchasing_site_flag
,rfq_only_site_flag
,pay_site_flag
,attention_ar_flag
,vendor_address1
,vendor_address2
,vendor_address3
,vendor_address4
,vendor_site_city
,vendor_site_state
,vendor_site_country
,ZIP
,vendor_site_phone
,vendor_site_fax
,payment_currency_code
,vendor_site_code_alt
,ship_to_location_id
,bill_to_location_id
,org_id
,location_id
,party_site_id
,tca_sync_state,
    freight_terms_lookup_code,
    inactive_date
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
    ( SELECT
        vendor_site_id,
        vendor_site_code,
        vendor_id,
        purchasing_site_flag,
        rfq_only_site_flag,
        pay_site_flag,
        attention_ar_flag,
        address_line1             "VENDOR_ADDRESS1",
        address_line2             "VENDOR_ADDRESS2",
        address_line3             "VENDOR_ADDRESS3",
        address_line4             "VENDOR_ADDRESS4",
        city                      "VENDOR_SITE_CITY",
        nvl(province, state)      "VENDOR_SITE_STATE",
        country                   "VENDOR_SITE_COUNTRY",
		ZIP,
        area_code
        || '-'
        || phone                  "VENDOR_SITE_PHONE",
        fax_area_code
        || '-'
        || fax                    "VENDOR_SITE_FAX",
        payment_currency_code,
        vendor_site_code_alt,
        ship_to_location_id,
        bill_to_location_id,
        org_id,
        location_id,
        party_site_id,
        tca_sync_state,
		    freight_terms_lookup_code,
    inactive_date,
		'N' as is_deleted_flg,
        (
            SELECT
                system_id
            FROM
                bec_etl_ctrl.etlsourceappid
            WHERE
                source_system = 'EBS'
        )                         AS source_app_id,
        (
            SELECT
                system_id
            FROM
                bec_etl_ctrl.etlsourceappid
            WHERE
                source_system = 'EBS'
        ) ||'-'|| nvl(vendor_site_id, 0) AS dw_load_id,
        getdate()                 AS dw_insert_date,
        getdate()                 AS dw_update_date
    FROM
        bec_ods.ap_supplier_sites_all
    WHERE
1=1 and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ap_supplier_sites' and batch_name = 'ap'))
    );
commit;

-- Soft delete

update bec_dwh.dim_ap_supplier_sites set is_deleted_flg = 'Y'
where nvl(vendor_site_id,0) not in (
select nvl(ods.vendor_site_id,0) from bec_dwh.dim_ap_supplier_sites dw, bec_ods.ap_supplier_sites_all ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.vendor_site_id,0) 
AND ods.is_deleted_flg <> 'Y');

commit;	

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ap_supplier_sites' and batch_name = 'ap';

COMMIT;