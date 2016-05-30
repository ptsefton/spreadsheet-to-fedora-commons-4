import unittest
from pcdmlite.pcdmlite import Item, Namespace 
from csv2pcdmlite.csv2pcdmlite import item_from_row
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef
from rdflib.namespace import DC, FOAF
import csv2f4




class TestPaths(unittest.TestCase):
    def test_paths(self):
        endpoint = "http://localhost:8080"
        loader = csv2f4.F4Loader(endpoint)
        

        self.assertEqual(loader.get_path("12345"), "/rest/objects/12345")
        self.assertEqual(loader.get_path("12345", is_collection=True), "/rest/collections/12345")

        o = item_from_row(row= {"dc:identifier": "1234"})
        loader.fedoraize_item(o)
        self.assertEqual(o.URL, "http://localhost:8080/rest/objects/1234")
        self.assertEqual(o.path, "/rest/objects/1234")
        self.assertEqual(o.id, "1234")
        
        c = item_from_row( {"dc:identifier": "my_collection", "dcterms:type": "pcdm:Collection"})
        loader.fedoraize_item(c)
        self.assertEqual(c.URL, "http://localhost:8080/rest/collections/my_collection")
        self.assertEqual(c.path, "/rest/collections/my_collection")
        self.assertEqual(c.id, "my_collection")
        
        

if __name__ == '__main__':
    unittest.main()
