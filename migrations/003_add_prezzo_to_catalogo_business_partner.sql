-- UP
ALTER TABLE sap.catalogo_business_partner ADD COLUMN IF NOT EXISTS prezzo DOUBLE PRECISION;

-- DOWN
ALTER TABLE sap.catalogo_business_partner DROP COLUMN IF EXISTS prezzo;
