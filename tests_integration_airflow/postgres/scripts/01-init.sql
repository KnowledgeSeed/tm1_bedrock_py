CREATE SCHEMA IF NOT EXISTS tm1_bedrock;

-- set search_path so that unqualified tables go into tm1_bedrock
SET search_path = tm1_bedrock;

-- example table: users
CREATE TABLE IF NOT EXISTS testbench_sales (
  id      SERIAL PRIMARY KEY,
  testbench_product    TEXT     NULL,
  testbench_customer   TEXT     NULL,
  testbench_key_account_manager   TEXT     NULL,
  testbench_measure_sales   TEXT     NULL,
  testbench_version   TEXT     NULL,
  testbench_period   TEXT     NULL,
  testbench_value    FLOAT   NULL
);