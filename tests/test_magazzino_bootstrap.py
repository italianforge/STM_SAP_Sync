import unittest
from unittest.mock import MagicMock

from src.sync.magazzino_bootstrap import (
    _BOOTSTRAP_MAGAZZINO_SQL,
    bootstrap_magazzino_from_sap,
)


class MagazzinoBootstrapTestCase(unittest.TestCase):
    def test_sql_uses_on_conflict_without_updating_quantita(self):
        self.assertIn('ON CONFLICT (articolo, posizione)', _BOOTSTRAP_MAGAZZINO_SQL)
        do_update = _BOOTSTRAP_MAGAZZINO_SQL.split('DO UPDATE SET', 1)[1]
        self.assertNotIn('quantita', do_update.lower())

    def test_bootstrap_returns_rowcount(self):
        session = MagicMock()
        session.execute.return_value.rowcount = 42

        result = bootstrap_magazzino_from_sap(session)

        self.assertEqual(result, 42)
        session.execute.assert_called_once()

    def test_bootstrap_treats_none_rowcount_as_zero(self):
        session = MagicMock()
        session.execute.return_value.rowcount = None

        result = bootstrap_magazzino_from_sap(session)

        self.assertEqual(result, 0)


if __name__ == '__main__':
    unittest.main()
