DROP table IF EXISTS BEC_SECURITY.EDP_USER_SECURITY;
CREATE TABLE BEC_SECURITY.EDP_USER_SECURITY AS
(
  SELECT
    eus.user_name,
    tu.email,
    eus.entity_type,
    CAST(eus.entity_id AS INT),
    CURRENT_TIMESTAMP() AS load_date
  FROM bec_security.ebs_user_security AS eus, bec_security.tableau_users AS tu
  WHERE
    1 = 1 AND UPPER(eus.email_address) = UPPER(tu.email)
);
INSERT INTO BEC_SECURITY.EDP_USER_SECURITY (
  USER_NAME,
  EMAIL,
  ENTITY_TYPE,
  ENTITY_ID,
  LOAD_DATE
)
SELECT
  B.USER_NAME,
  B.EMAIL,
  A.ENTITY_TYPE,
  CAST(A.ENTITY_ID AS INT),
  CURRENT_TIMESTAMP() AS LOAD_DATE
FROM BEC_SECURITY.EDP_USER_ROLES AS A, BEC_SECURITY.EDP_SECURITY_STAGE AS B
WHERE
  A.USER_ROLE = B.USER_ROLE;
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP(), load_type = 'I'
WHERE
  dw_table_name = 'edp_user_security' AND batch_name = 'security';