TRUNCATE table
	table bronze_bec_ods_stg.gl_code_combinations;
INSERT INTO bronze_bec_ods_stg.gl_code_combinations (
  CODE_COMBINATION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CHART_OF_ACCOUNTS_ID,
  DETAIL_POSTING_ALLOWED_FLAG,
  DETAIL_BUDGETING_ALLOWED_FLAG,
  ACCOUNT_TYPE,
  ENABLED_FLAG,
  SUMMARY_FLAG,
  SEGMENT1,
  SEGMENT2,
  SEGMENT3,
  SEGMENT4,
  SEGMENT5,
  SEGMENT6,
  SEGMENT7,
  SEGMENT8,
  SEGMENT9,
  SEGMENT10,
  SEGMENT11,
  SEGMENT12,
  SEGMENT13,
  SEGMENT14,
  SEGMENT15,
  SEGMENT16,
  SEGMENT17,
  SEGMENT18,
  SEGMENT19,
  SEGMENT20,
  SEGMENT21,
  SEGMENT22,
  SEGMENT23,
  SEGMENT24,
  SEGMENT25,
  SEGMENT26,
  SEGMENT27,
  SEGMENT28,
  SEGMENT29,
  SEGMENT30,
  DESCRIPTION,
  TEMPLATE_ID,
  ALLOCATION_CREATE_FLAG,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  CONTEXT,
  SEGMENT_ATTRIBUTE1,
  SEGMENT_ATTRIBUTE2,
  SEGMENT_ATTRIBUTE3,
  SEGMENT_ATTRIBUTE4,
  SEGMENT_ATTRIBUTE5,
  SEGMENT_ATTRIBUTE6,
  SEGMENT_ATTRIBUTE7,
  SEGMENT_ATTRIBUTE8,
  SEGMENT_ATTRIBUTE9,
  SEGMENT_ATTRIBUTE10,
  SEGMENT_ATTRIBUTE11,
  SEGMENT_ATTRIBUTE12,
  SEGMENT_ATTRIBUTE13,
  SEGMENT_ATTRIBUTE14,
  SEGMENT_ATTRIBUTE15,
  SEGMENT_ATTRIBUTE16,
  SEGMENT_ATTRIBUTE17,
  SEGMENT_ATTRIBUTE18,
  SEGMENT_ATTRIBUTE19,
  SEGMENT_ATTRIBUTE20,
  SEGMENT_ATTRIBUTE21,
  SEGMENT_ATTRIBUTE22,
  SEGMENT_ATTRIBUTE23,
  SEGMENT_ATTRIBUTE24,
  SEGMENT_ATTRIBUTE25,
  SEGMENT_ATTRIBUTE26,
  SEGMENT_ATTRIBUTE27,
  SEGMENT_ATTRIBUTE28,
  SEGMENT_ATTRIBUTE29,
  SEGMENT_ATTRIBUTE30,
  SEGMENT_ATTRIBUTE31,
  SEGMENT_ATTRIBUTE32,
  SEGMENT_ATTRIBUTE33,
  SEGMENT_ATTRIBUTE34,
  SEGMENT_ATTRIBUTE35,
  SEGMENT_ATTRIBUTE36,
  SEGMENT_ATTRIBUTE37,
  SEGMENT_ATTRIBUTE38,
  SEGMENT_ATTRIBUTE39,
  SEGMENT_ATTRIBUTE40,
  SEGMENT_ATTRIBUTE41,
  SEGMENT_ATTRIBUTE42,
  JGZZ_RECON_FLAG,
  JGZZ_RECON_CONTEXT,
  REFERENCE1,
  REFERENCE2,
  REFERENCE3,
  REFERENCE4,
  REFERENCE5,
  PRESERVE_FLAG,
  REFRESH_FLAG,
  IGI_BALANCED_BUDGET_FLAG,
  COMPANY_COST_CENTER_ORG_ID,
  REVALUATION_ID,
  LEDGER_SEGMENT,
  LEDGER_TYPE_CODE,
  ALTERNATE_CODE_COMBINATION_ID,
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    CODE_COMBINATION_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CHART_OF_ACCOUNTS_ID,
    DETAIL_POSTING_ALLOWED_FLAG,
    DETAIL_BUDGETING_ALLOWED_FLAG,
    ACCOUNT_TYPE,
    ENABLED_FLAG,
    SUMMARY_FLAG,
    SEGMENT1,
    SEGMENT2,
    SEGMENT3,
    SEGMENT4,
    SEGMENT5,
    SEGMENT6,
    SEGMENT7,
    SEGMENT8,
    SEGMENT9,
    SEGMENT10,
    SEGMENT11,
    SEGMENT12,
    SEGMENT13,
    SEGMENT14,
    SEGMENT15,
    SEGMENT16,
    SEGMENT17,
    SEGMENT18,
    SEGMENT19,
    SEGMENT20,
    SEGMENT21,
    SEGMENT22,
    SEGMENT23,
    SEGMENT24,
    SEGMENT25,
    SEGMENT26,
    SEGMENT27,
    SEGMENT28,
    SEGMENT29,
    SEGMENT30,
    DESCRIPTION,
    TEMPLATE_ID,
    ALLOCATION_CREATE_FLAG,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    ATTRIBUTE10,
    CONTEXT,
    SEGMENT_ATTRIBUTE1,
    SEGMENT_ATTRIBUTE2,
    SEGMENT_ATTRIBUTE3,
    SEGMENT_ATTRIBUTE4,
    SEGMENT_ATTRIBUTE5,
    SEGMENT_ATTRIBUTE6,
    SEGMENT_ATTRIBUTE7,
    SEGMENT_ATTRIBUTE8,
    SEGMENT_ATTRIBUTE9,
    SEGMENT_ATTRIBUTE10,
    SEGMENT_ATTRIBUTE11,
    SEGMENT_ATTRIBUTE12,
    SEGMENT_ATTRIBUTE13,
    SEGMENT_ATTRIBUTE14,
    SEGMENT_ATTRIBUTE15,
    SEGMENT_ATTRIBUTE16,
    SEGMENT_ATTRIBUTE17,
    SEGMENT_ATTRIBUTE18,
    SEGMENT_ATTRIBUTE19,
    SEGMENT_ATTRIBUTE20,
    SEGMENT_ATTRIBUTE21,
    SEGMENT_ATTRIBUTE22,
    SEGMENT_ATTRIBUTE23,
    SEGMENT_ATTRIBUTE24,
    SEGMENT_ATTRIBUTE25,
    SEGMENT_ATTRIBUTE26,
    SEGMENT_ATTRIBUTE27,
    SEGMENT_ATTRIBUTE28,
    SEGMENT_ATTRIBUTE29,
    SEGMENT_ATTRIBUTE30,
    SEGMENT_ATTRIBUTE31,
    SEGMENT_ATTRIBUTE32,
    SEGMENT_ATTRIBUTE33,
    SEGMENT_ATTRIBUTE34,
    SEGMENT_ATTRIBUTE35,
    SEGMENT_ATTRIBUTE36,
    SEGMENT_ATTRIBUTE37,
    SEGMENT_ATTRIBUTE38,
    SEGMENT_ATTRIBUTE39,
    SEGMENT_ATTRIBUTE40,
    SEGMENT_ATTRIBUTE41,
    SEGMENT_ATTRIBUTE42,
    JGZZ_RECON_FLAG,
    JGZZ_RECON_CONTEXT,
    REFERENCE1,
    REFERENCE2,
    REFERENCE3,
    REFERENCE4,
    REFERENCE5,
    PRESERVE_FLAG,
    REFRESH_FLAG,
    IGI_BALANCED_BUDGET_FLAG,
    COMPANY_COST_CENTER_ORG_ID,
    REVALUATION_ID,
    LEDGER_SEGMENT,
    LEDGER_TYPE_CODE,
    ALTERNATE_CODE_COMBINATION_ID,
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.gl_code_combinations
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (code_combination_id, kca_seq_id) IN (
      SELECT
        code_combination_id,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_CODE_COMBINATIONS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        code_combination_id
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_code_combinations'
      )
    )
);