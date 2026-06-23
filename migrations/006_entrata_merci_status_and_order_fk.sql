-- UP
ALTER TABLE sap.entrata_merci
    ADD COLUMN IF NOT EXISTS status VARCHAR(10);

ALTER TABLE sap.entrata_merci_lines
    ADD COLUMN IF NOT EXISTS cod_order_acquisto INTEGER,
    ADD COLUMN IF NOT EXISTS status VARCHAR(10);

ALTER TABLE sap.entrata_merci_lines
    DROP CONSTRAINT IF EXISTS entrata_merci_lines_cod_order_acquisto_fkey;

ALTER TABLE sap.entrata_merci_lines
    ADD CONSTRAINT entrata_merci_lines_cod_order_acquisto_fkey
    FOREIGN KEY (cod_order_acquisto)
    REFERENCES sap.ordini_acquisto (id);

-- DOWN
ALTER TABLE sap.entrata_merci_lines
    DROP CONSTRAINT IF EXISTS entrata_merci_lines_cod_order_acquisto_fkey;

ALTER TABLE sap.entrata_merci_lines
    DROP COLUMN IF EXISTS cod_order_acquisto,
    DROP COLUMN IF EXISTS status;

ALTER TABLE sap.entrata_merci
    DROP COLUMN IF EXISTS status;
