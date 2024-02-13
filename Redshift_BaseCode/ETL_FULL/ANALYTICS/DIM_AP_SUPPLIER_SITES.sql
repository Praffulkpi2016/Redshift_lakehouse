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
drop table if exists bec_dwh.DIM_AP_SUPPLIER_SITES;

CREATE TABLE bec_dwh.DIM_AP_SUPPLIER_SITES 
diststyle all 
sortkey(vendor_id,vendor_site_id)
AS
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
    zip,
    nvl(province, state)      "VENDOR_SITE_STATE",
    country                   "VENDOR_SITE_COUNTRY",
    area_code || '-'
                 || phone                  "VENDOR_SITE_PHONE",
    fax_area_code || '-'
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
    'N'                       AS is_deleted_flg,
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
    )
    || '-'
       || nvl(vendor_site_id, 0) AS dw_load_id,
    getdate()                 AS dw_insert_date,
    getdate()                 AS dw_update_date
FROM
    bec_ods.ap_supplier_sites_all
);

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
        dw_table_name = 'dim_ap_supplier_sites'
    AND batch_name = 'ap';

COMMIT;
