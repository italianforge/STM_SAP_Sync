import unittest
from urllib.parse import unquote_plus

from src.config.database import _build_mssql_url_from_fields


class MssqlUrlBuilderTestCase(unittest.TestCase):
    def test_named_instance_uses_odbc_connect(self):
        url = _build_mssql_url_from_fields(
            r'192.168.0.5\MODULA',
            '1433',
            'SYSTOREDB',
            'modula_read',
            'M0dula@01',
        )
        self.assertTrue(url.startswith('mssql+pyodbc:///?odbc_connect='))
        connect = unquote_plus(url.split('odbc_connect=', 1)[1])
        self.assertIn('SERVER=192.168.0.5\\MODULA', connect)
        self.assertIn('DATABASE=SYSTOREDB', connect)
        self.assertNotIn('TrustServerCertificate', connect)

    def test_modern_driver_adds_trust_server_certificate(self):
        url = _build_mssql_url_from_fields(
            r'192.168.0.5\MODULA',
            '1433',
            'SYSTOREDB',
            'modula_read',
            'M0dula@01',
            'ODBC Driver 18 for SQL Server',
        )
        connect = unquote_plus(url.split('odbc_connect=', 1)[1])
        self.assertIn('TrustServerCertificate=yes', connect)

    def test_default_instance_keeps_host_url_form(self):
        url = _build_mssql_url_from_fields(
            '192.168.0.5',
            '1433',
            'DBDATA',
            'user',
            'pass',
        )
        self.assertIn('@192.168.0.5/DBDATA', url)
        self.assertNotIn('TrustServerCertificate', url)


if __name__ == '__main__':
    unittest.main()
