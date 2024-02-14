/* Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.# Unless required by applicable law or agreed to in writing, softwaredistributed under the License is distributed on an "AS IS" BASIS, WITHOUTWARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.# author: KPI Partners, Inc.version: 2022.06description: This script represents full load approach for stage.File Version: KPI v1.0
*/
DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_BUDGET_VERSIONS;
CREATE TABLE bronze_bec_ods_stg.GL_BUDGET_VERSIONS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_BUDGET_VERSIONS
WHERE
  kca_operation <> 'DELETE'
  AND (BUDGET_VERSION_ID, last_update_date) IN (
    SELECT
      BUDGET_VERSION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_BUDGET_VERSIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      BUDGET_VERSION_ID
  );