import unittest

from src.sync.modula_enrichment import _load_modula_by_description


class ModulaEnrichmentTestCase(unittest.TestCase):
    def test_load_modula_by_description_skips_empty_description(self):
        class FakeResult:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        class FakeSession:
            def execute(self, _query):
                return FakeResult([
                    ('ART001', 'Vite M6'),
                    ('ART002', ''),
                    ('ART003', None),
                ])

        index = _load_modula_by_description(FakeSession())
        self.assertEqual(index, {'Vite M6': ['ART001']})

    def test_load_modula_by_description_supports_duplicate_descriptions(self):
        class FakeResult:
            def fetchall(self):
                return [('ART001', 'Same Desc'), ('ART002', 'Same Desc')]

        class FakeSession:
            def execute(self, _query):
                return FakeResult()

        index = _load_modula_by_description(FakeSession())
        self.assertEqual(sorted(index['Same Desc']), ['ART001', 'ART002'])


if __name__ == '__main__':
    unittest.main()
