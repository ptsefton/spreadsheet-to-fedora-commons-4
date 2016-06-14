#!/usr/bin/python
"""
Uploads a CSV file to a Fedora server

"""
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import object
import json
import tablib
import yaml
import argparse
from sys import stdin
from sys import stdout
import sys
import httplib2
import os
import urllib.parse
import requests
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef
from rdflib.namespace import DC, FOAF
import logging
import base64
from pcdmlite.pcdmlite import Item, Namespace
from csv2pcdmlite.csv2pcdmlite import CSVData, Field, populate_item_from_row
import fcrepo4



        
class F4Loader(object):
    """ Object to connect to Fedora, handle all the path generation etc  """
    
    def get_path(self, id,  is_collection = False):
        """ Generate a fedora path for objects / collections """
        type_of_things = "collections" if is_collection else "objects"
        ##TODO: URL escape ID
        
        return("%s/%s" % (type_of_things, id))

 
        
    def __init__(self, endpoint, data_dir = "./data", quietly = False, csv_file = None):
        """ Create the loader object:
            data_dir: where to keep temp-downloads, consider keeping this somwhere you can re-use, as it is kind of a cache
            quietly: Suppress most of the logging chatter
            csv_file: Stream object containing tab-separated CSV
        """
        self.endpoint = endpoint
        self.data_dir = data_dir
        self.csv_file = csv_file
        self.logger = self.create_stream_logger('csv2f4', stdout)
        if quietly:
            self.logger.setLevel(30)

        if csv_file:
            self.csv_data = CSVData(csv_file)
            self.csv_data.get_items()
    
    def fedoraize_item(self, item):
        """ Create new fedora-specific properties for a generic repository item """
        item.path = self.get_path(item.id, item.is_collection)
        #item.URL = self.get_url(item.path)
        item.files_path = "%s/files" % item.path # TODO: make a function!


    # Private methods for Logging
    # -------------------------------------------------------------------

    def __create_logger(self, loggername, handler):
        logger = logging.getLogger(loggername)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        return logger


    def __create_formatter(self):
        return logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')


    def create_stream_logger(self, loggername, stream=sys.stdout):
        # Create the required handler
        handler = logging.StreamHandler(stream)
        handler.setLevel(logging.INFO)
        handler.setFormatter(self.__create_formatter())

        #create (and return) a new logger using the handler.
        return self.__create_logger(loggername, handler)
    # end create_stream_logger(loggername, stream=sys.stdout)

 


       

    def resource_exists(self, url):
        res = requests.get(url)
        self.logger.info("Checking existence of %s : %s", url, res.ok)
        return res.ok

    def load(self):
        """
        Push the contents of the previously read CSV file (now a list of PCDM collections and objects) into Fedora
        """
        repo = fcrepo4.Repository(config='config.yml')
        root = repo.get(repo.path2uri('/'))
        collections = {}
        for collection in self.csv_data.collections:
            self.fedoraize_item(collection)
            collection_name = collection.id
            collections[collection.id] = collection
            coll = root.add_container(collection.graph, path = collection.path, force=True)
            
        for item in self.csv_data.items:
            self.fedoraize_item(item)
            id = item.id

            if id != None:
                title = item.title
                type = item.type
                #TODO
                #previous_id = omeka_client.get_item_id_by_dc_identifier(id)
                item_path = item.path
                #item_url = item.URL
                
                print("Processing item:", item.path)
           
                # Deal with relations between objects
                for rel in item.relations:
                    object_id = rel.value
                    object_path = self.get_path(object_id)
                    #item.graph.add((URIRef(""), URIRef(rel.qualified_name), URIRef(repo.path2reluri(object_path))))
                    #TODO _ back-relations?
                    
                collection = None
                collection_id = item.in_collection
                if  collection_id and collection_id in collections:
                    print("Adding to collection", collection_id)
                    collection = collections[collection_id]
                    item.graph.add((URIRef(""), URIRef("http://pcdm.org/models#memberOf"), URIRef(repo.path2reluri(collection.path))))
                # Upload it
                fedora_item = root.add_container(item.graph, path = item.path, force=True)

                
                
                if  collection:
                    print("Adding to collection", collection_id)
                    collection = collections[collection_id] # Local pcdmi collection object cached
                    coll = repo.get(repo.path2uri(collection.path))
                    coll.rdf_add(URIRef("http://pcdm.org/models#hasMember"),       URIRef(repo.path2reluri(item.path)))
                    coll.rdf_write()

                
                for url_field in item.URLs:        
                    filename = urllib.parse.urlsplit(url_field.value).path.split("/")[-1]
                    fedora_item.add_binary(url_field.value, path=filename)


                for f in item.files:
                     fyle = os.path.join(self.data_dir, f.value)
                     _,fylename = os.path.split(f.value)
                     if os.path.exists(fyle):
                            fedora_item.add_binary(fyle, path=fylename)
               


   
if __name__ == "__main__":
    # Define and parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('inputfile', type=argparse.FileType('r'),  default=stdin, help='Name of input CSV file')
    parser.add_argument('-u', '--api_url',default="http://localhost:8080", help='Fedora URL')
    parser.add_argument('-d', '--download_cache', default="./data", help='Path to a directory in which to chache dowloads (defaults to ./data)')
    parser.add_argument('-q', '--quietly', action='store_true', help='Only log errors and warnings not the constant stream of info')
    args = vars(parser.parse_args())


    endpoint = args['api_url'] 
    csv_file = args['inputfile']
    quietly = args['quietly']
    data_dir = args['download_cache']

    f4 = F4Loader(endpoint, data_dir, quietly, csv_file)
    f4.load()





