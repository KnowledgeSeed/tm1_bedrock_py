CREATE SCHEMA IF NOT EXISTS tm1_bedrock;

-- set search_path so that unqualified tables go into tm1_bedrock
SET search_path = tm1_bedrock;

-- example table: users
CREATE TABLE IF NOT EXISTS testbenchSales (
  id      SERIAL PRIMARY KEY,
  testbenchProduct    TEXT     NULL,
  testbenchCustomer   TEXT     NULL,
  testbenchKeyAccountManager   TEXT     NULL,
  testbenchMeasureSales   TEXT     NULL,
  testbenchVersion   TEXT     NULL,
  testbenchPeriod   TEXT     NULL,
  testbenchValue    FLOAT   NULL
);