import unittest

from src.mappings.catalogo_business_partner import (
    MAPPING_CATALOGO_BUSINESS_PARTNER,
    _CATALOGO_BUSINESS_PARTNER_QUERY,
)


class CatalogoBusinessPartnerMappingTestCase(unittest.TestCase):
    def test_query_joins_spp1_on_itemcode_and_cardcode(self):
        query = _CATALOGO_BUSINESS_PARTNER_QUERY.upper()
        self.assertIn("FROM DBO.OSCN", query)
        self.assertIn("FROM DBO.SPP1", query)
        self.assertIn("ITEMCODE = O.ITEMCODE", query)
        self.assertIn("CARDCODE = O.CARDCODE", query)
        self.assertIn("PRICE", query)

    def test_mapping_includes_prezzo(self):
        row = MAPPING_CATALOGO_BUSINESS_PARTNER.transform_row({
            "ItemCode": "ART001",
            "CardCode": "F001",
            "Substitute": "TR001",
            "Price": 12.5,
        })
        self.assertEqual(row["cod_articolo"], "ART001")
        self.assertEqual(row["cod_business_partner"], "F001")
        self.assertEqual(row["translation"], "TR001")
        self.assertEqual(row["prezzo"], 12.5)

    def test_mapping_handles_missing_price(self):
        row = MAPPING_CATALOGO_BUSINESS_PARTNER.transform_row({
            "ItemCode": "ART001",
            "CardCode": "F001",
            "Substitute": "TR001",
            "Price": None,
        })
        self.assertIsNone(row["prezzo"])


if __name__ == '__main__':
    unittest.main()
