-- UP
ALTER TABLE sap.entrata_merci_lines
    DROP CONSTRAINT IF EXISTS entrata_merci_lines_cod_entrata_merci_fkey;

ALTER TABLE sap.entrata_merci_lines
    ADD CONSTRAINT entrata_merci_lines_cod_entrata_merci_fkey
    FOREIGN KEY (cod_entrata_merci)
    REFERENCES sap.entrata_merci (id)
    ON DELETE CASCADE;

-- DOWN
ALTER TABLE sap.entrata_merci_lines
    DROP CONSTRAINT IF EXISTS entrata_merci_lines_cod_entrata_merci_fkey;
