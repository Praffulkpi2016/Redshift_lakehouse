/* Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.# Unless required by applicable law or agreed to in writing, softwaredistributed under the License is distributed on an "AS IS" BASIS, WITHOUTWARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.# author: KPI Partners, Inc.version: 2022.06description: This script represents full load approach for stage.File Version: KPI v1.0
*/
DROP TABLE IF EXISTS bronze_bec_ods_stg.XXBEC_SRVCREV_CDW_MASTER_STG;
CREATE TABLE bronze_bec_ods_stg.XXBEC_SRVCREV_CDW_MASTER_STG AS
SELECT
  *
FROM bec_raw_dl_ext.XXBEC_SRVCREV_CDW_MASTER_STG
WHERE
  kca_operation <> 'DELETE';