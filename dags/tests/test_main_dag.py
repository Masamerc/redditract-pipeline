from airflow.models import DagBag
from unittest import TestCase, TestLoader, TextTestRunner

# write tests for the dag here

class TestDagIntegrity(TestCase):

    def setUp(self):
        self.dagbag = DagBag()

    def test_import_dags(self):
        '''smoketest for the dagbag'''
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failed. Errors: {}'.format(self.dagbag.import_errors)
        )

if __name__ == "__main__":
    suite = TestLoader().loadTestsFromTestCase(TestDagIntegrity)
    TextTestRunner(verbosity=2).run(suite)
