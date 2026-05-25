-- UP
ALTER TABLE sap.catalogo_business_partner DROP COLUMN IF EXISTS email;
ALTER TABLE sap.anagrafica_business_partner ADD COLUMN IF NOT EXISTS email VARCHAR;

-- DOWN
ALTER TABLE sap.anagrafica_business_partner DROP COLUMN IF EXISTS email;
