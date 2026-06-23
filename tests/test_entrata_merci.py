import unittest

from src.mappings.entrata_merci import MAPPING_ENTRATA_MERCI, _ENTRATA_MERCI_QUERY
from src.mappings.entrata_merci_lines import (
    MAPPING_ENTRATA_MERCI_LINES,
    _ENTRATA_MERCI_LINES_QUERY,
)


class EntrataMerciMappingTestCase(unittest.TestCase):
    def test_header_query_reads_from_opdn(self):
        query = _ENTRATA_MERCI_QUERY.upper()
        self.assertIn("FROM DBO.OPDN", query)
        self.assertIn("DOCENTRY", query)
        self.assertIn("DOCDATE", query)
        self.assertIn("CARDCODE", query)
        self.assertIn("DOCSTATUS", query)

    def test_header_mapping_transforms_row(self):
        row = MAPPING_ENTRATA_MERCI.transform_row({
            "DocEntry": 1001,
            "DocDate": "2026-01-15",
            "CardCode": "F001",
            "DocStatus": "C",
        })
        self.assertEqual(row["id"], 1001)
        self.assertEqual(row["cod_business_partner"], "F001")
        self.assertEqual(row["status"], "CLOSED")
        self.assertIsNotNone(row["date_registration"])

    def test_header_mapping_open_status(self):
        row = MAPPING_ENTRATA_MERCI.transform_row({
            "DocEntry": 1002,
            "DocDate": "2026-01-15",
            "CardCode": "F002",
            "DocStatus": "O",
        })
        self.assertEqual(row["status"], "OPEN")


class EntrataMerciLinesMappingTestCase(unittest.TestCase):
    def test_lines_query_joins_pdn1_and_opdn(self):
        query = _ENTRATA_MERCI_LINES_QUERY.upper()
        self.assertIn("FROM DBO.PDN1", query)
        self.assertIn("INNER JOIN DBO.OPDN", query)
        self.assertIn("L.DOCENTRY = H.DOCENTRY", query)
        self.assertIn("ITEMCODE", query)
        self.assertIn("QUANTITY", query)
        self.assertIn("BASEENTRY", query)
        self.assertIn("LINESTATUS", query)

    def test_lines_mapping_transforms_row(self):
        row = MAPPING_ENTRATA_MERCI_LINES.transform_row({
            "DocEntry": 1001,
            "LineNum": 0,
            "ItemCode": "ART001",
            "Quantity": 3.5,
            "BaseEntry": 501,
            "LineStatus": "C",
        })
        self.assertEqual(row["cod_entrata_merci"], 1001)
        self.assertEqual(row["line_num"], 0)
        self.assertEqual(row["cod_articolo"], "ART001")
        self.assertEqual(row["quantity"], 3.5)
        self.assertEqual(row["cod_order_acquisto"], 501)
        self.assertEqual(row["status"], "CLOSED")

    def test_lines_mapping_null_order_when_base_entry_zero(self):
        row = MAPPING_ENTRATA_MERCI_LINES.transform_row({
            "DocEntry": 1001,
            "LineNum": 1,
            "ItemCode": "ART002",
            "Quantity": 1,
            "BaseEntry": 0,
            "LineStatus": "O",
        })
        self.assertIsNone(row["cod_order_acquisto"])
        self.assertEqual(row["status"], "OPEN")


if __name__ == '__main__':
    unittest.main()
