-- UP
DROP TABLE IF EXISTS sap.entrata_merci_lines;
DROP TABLE IF EXISTS sap.entrata_merci;

CREATE TABLE sap.entrata_merci (
    id INTEGER NOT NULL PRIMARY KEY,
    date_registration TIMESTAMP,
    cod_business_partner VARCHAR(50)
);

CREATE TABLE sap.entrata_merci_lines (
    cod_entrata_merci INTEGER NOT NULL,
    line_num INTEGER NOT NULL,
    cod_articolo VARCHAR(50),
    quantity DOUBLE PRECISION DEFAULT 0,
    PRIMARY KEY (cod_entrata_merci, line_num)
);

-- DOWN
DROP TABLE IF EXISTS sap.entrata_merci_lines;
DROP TABLE IF EXISTS sap.entrata_merci;
