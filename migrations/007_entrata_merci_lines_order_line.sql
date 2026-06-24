-- UP
ALTER TABLE sap.entrata_merci_lines
    ADD COLUMN IF NOT EXISTS order_line INTEGER;

-- DOWN
ALTER TABLE sap.entrata_merci_lines
    DROP COLUMN IF EXISTS order_line;
