import unittest

from src.sync.deposyta_enrichment import normalizza_qta_confezione, pezzi_da_confezioni


class DeposytaEnrichmentTestCase(unittest.TestCase):
    def test_normalizza_qta_confezione_default_uno(self):
        self.assertEqual(normalizza_qta_confezione(None), 1.0)
        self.assertEqual(normalizza_qta_confezione(0), 1.0)
        self.assertEqual(normalizza_qta_confezione(-5), 1.0)

    def test_normalizza_qta_confezione_valore_valido(self):
        self.assertEqual(normalizza_qta_confezione(10), 10.0)

    def test_pezzi_da_confezioni_moltiplica_per_qta_confezione(self):
        self.assertEqual(pezzi_da_confezioni(3, 10), 30.0)

    def test_pezzi_da_confezioni_qta_confezione_zero_usa_uno(self):
        self.assertEqual(pezzi_da_confezioni(5, 0), 5.0)

    def test_pezzi_da_confezioni_none_se_quantita_assente(self):
        self.assertIsNone(pezzi_da_confezioni(None, 10))


if __name__ == '__main__':
    unittest.main()
