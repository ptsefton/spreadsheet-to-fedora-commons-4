import unittest
import sys
sys.path.append("../omeka-python-utils")
from csv2repo import CSVData, Field, Item, Namespace
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef
from rdflib.namespace import DC, FOAF
import csv2f4




class TestPaths(unittest.TestCase):
    def test_object_path(self):
        endpoint = "http://localhost:8080"
        loader = csv2f4.F4Loader(endpoint)
        o = csv2f4.F4Object(loader, id="1234")
        self.assertEqual(o.URL, "http://localhost:8080/rest/objects/1234")
        self.assertEqual(o.path, "/rest/objects/1234")
        self.assertEqual(o.id, "1234")


        o = csv2f4.F4Object(loader, row = {"dc:identifier": "1234"})
        self.assertEqual(o.URL, "http://localhost:8080/rest/objects/1234")
        self.assertEqual(o.path, "/rest/objects/1234")
        self.assertEqual(o.id, "1234")

if __name__ == '__main__':
    unittest.main()
