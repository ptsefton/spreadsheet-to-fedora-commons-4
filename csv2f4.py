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




        
class F4Loader(object):
    """ Object to connect to Fedora, handle all the path generation etc. Demo code only, to be fixed up with 
        the new Fedora API Mike Lynch is developing"""
    
    def get_path(self, id,  is_collection = False):
        """ Generate a fedora path for objects / collections """
        type_of_things = "collections" if is_collection else "objects"
        ##TODO: URL escape ID
        
        return("/rest/%s/%s" % (type_of_things, id))

    def get_url(self, path):
        """ Add endpoint to front of path to get URL"""
        return("%s%s" % (self.endpoint, path))
        
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
        item.URL = self.get_url(item.path)
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

    def download_and_upload_files(self, item):
        """
        Handle any dowloads, cache as files locally in data dir, then upload all files
        item: An pcdm_lite item object

        """
        http = httplib2.Http()
        download_this = True
        files = []
        for url_field in item.URLs:
            url = url_field.value
            filename = urllib.parse.urlsplit(url).path.split("/")[-1]
            new_path = os.path.join(data_dir, str(item.id))
            if not os.path.exists(new_path):
                os.makedirs(new_path)
            file_path = os.path.join(new_path, filename)
            self.logger.info("Local filename: %s", file_path)

            #Check if we have one the same size already
            if os.path.exists(file_path):
                response, content = http.request(url, "HEAD")
                download_size = int(response['content-length']) if 'content-length' in response else -1
                file_size = os.path.getsize(file_path)
                if download_size == file_size:
                    self.logger.info("Already have a download of the same size: %d", file_size)
                    download_this = False

            if download_this:
                try:
                    response, content = http.request(url, "GET")
                    open(file_path,'wb').write(content)
                    self.logger.info(response)
                except:
                    self.logger.warning("Some kind of download error happened fetching %s - pressing on" % url)

            files.append(file_path)


        for f in item.files:
            files.append(os.path.join(data_dir, f.value))

        for fyle in files:
            if os.path.exists(fyle):
                self.logger.info("Uploading %s", fyle)
                files_url = "%s/files" % (item.URL)
                headers = {"Content-Type": "text/turtle", "slug": "files"}
                if not requests.get(files_url).ok : 
                    r = requests.post(url = item.URL, headers=headers)
                    print(("**********", url, r, r.text))
                (path, filename) = os.path.split(fyle)
                file_url = "%s/%s" % (files_url, filename)
                headers={'Content-Type': 'application/octet-stream'}
                r = requests.put(url = file_url, data=open(fyle, 'rb').read(), headers=headers)
                
                self.logger.info("Respoinse: \n%s \n%s \n%s", r, r.text, r.reason)
                self.logger.info("Uploaded %s %s", file_url, fyle)

    def resource_exists(self, url):
        res = requests.get(url)
        self.logger.info("Checking existence of %s : %s", url, res.ok)
        return res.ok

    def load(self):
        """
        Push the contents of the previously read CSV file (now a list of PCDM collections and objects) into Fedora
        """
        
        collections = {}
        for collection in self.csv_data.collections:
            self.fedoraize_item(collection)
            collection_name = collection.id
            collections[collection.id] = collection
            print ("Processing potential collection: %s" % collection_name)
            data = collection.serialize_RDF()[2:-1].replace("\\n", "\n")
            print(data)
            headers = {'Content-type': 'text/turtle', 'Slug': collection_name, 'Accept': 'text/turtle'}
            res = requests.delete(url = collection.URL)
            res = requests.delete(url = collection.URL + "/fcr:tombstone")
            res = requests.put(url = collection.URL,
                                headers = headers,
                                data = data)
            print("Trying to create collection", collection.URL, res.text)
            print(res.status_code, res.reason)
            



        for item in self.csv_data.items:
            self.fedoraize_item(item)
            id = item.id

            if id != None:
                title = item.title
                type = item.type
                #TODO
                #previous_id = omeka_client.get_item_id_by_dc_identifier(id)
                item_path = item.path
                item_url = item.URL
                print("Processing item:", item_url)
                collection_id = item.in_collection
                if  collection_id and collection_id in collections:
                    print("Adding to collection", collection_id)
                    collection = collections[collection_id]
                    collection_path = collection.path
                    if self.resource_exists(collection.URL):
                        item.graph.add((Literal("<>"), URIRef("http://pcdm.org/models#memberOf"), URIRef("%s" % collection_path)))

                # Deal with relations between objects
                for rel in item.relations:
                    object_id = rel.value
                    object_path = self.get_path(object_id)
                    object_url = self.get_url(object_path)
                    #print "Relating"
                    if self.resource_exists(object_url):
                        item.graph.add((Literal("<>"),URIRef(rel.qualified_name), URIRef(object_path)))
                    #TODO _ back-relations?

                # Upload it
                data = item.serialize_RDF()[2:-1].replace("\\n","\n")
                headers = {'Content-type': 'text/turtle', 'Accept': 'text/turtle'}
                if self.resource_exists(item.URL):
                    data = requests.get(item_url).text + data
                else:
                     print(("Minting", id))
                     headers['slug'] = id
                res = requests.put(url = item_url,
                                    headers = headers,
                                    data = data)
                
                self.logger.info("Put object: %s : %s %s ", res, res.text, res.reason)
                if item.in_collection and self.resource_exists(collection.URL):
                    headers = {"Content-Type": "text/turtle"}
                    membership = "\n<> <http://pcdm.org/models#hasMember> <%s> \n.\n" % (item_path)
                    new_collection_data = requests.get(collection.URL).text + membership
                    res = requests.put(url = collection.URL, data = new_collection_data, headers=headers)
                    print("Posted new collection details", res, res.text ,res.reason)


                print (item_url)
                #print requests.get(item_url).text
                self.download_and_upload_files(item)


   
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





