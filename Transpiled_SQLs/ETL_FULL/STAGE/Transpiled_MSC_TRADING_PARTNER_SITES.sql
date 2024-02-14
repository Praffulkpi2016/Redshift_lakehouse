/* Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.# Unless required by applicable law or agreed to in writing, softwaredistributed under the License is distributed on an "AS IS" BASIS, WITHOUTWARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.# author: KPI Partners, Inc.version: 2022.06description: This script represents full load approach for stage.File Version: KPI v1.0
*/
DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_TRADING_PARTNER_SITES;
CREATE TABLE bronze_bec_ods_stg.MSC_TRADING_PARTNER_SITES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MSC_TRADING_PARTNER_SITES
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(PARTNER_SITE_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(PARTNER_SITE_ID, 0) AS PARTNER_SITE_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MSC_TRADING_PARTNER_SITES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(PARTNER_SITE_ID, 0)
    )
);