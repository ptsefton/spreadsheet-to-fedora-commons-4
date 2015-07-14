#!/usr/bin/python
"""
Uploads a CSV file to a Fedora server



"""
import json
import tablib
import yaml
import argparse
from sys import stdin
from sys import stdout
import sys
import httplib2
import os
import urlparse
import requests
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef
from rdflib.namespace import DC, FOAF
import logging
import base64
sys.path.append("../omeka-python-utils")
from csv2repo import CSVData, Field, Item, Namespace
import csv2repo

class F4Object(csv2repo.Item):
    
    def __init__(self, loader, row = {}, id = None):
        csv2repo.Item.__init__(self,row = row)
        self.loader = loader
        
        if row == {} and id:
            self.id = id
        self.loader = loader
        self.__set_paths()
        
    def __set_paths(self):
        self.URL = "%s/rest/objects/%s" % (self.loader.endpoint, self.id)
        self.path = "/rest/objects/%s" % self.id
        self.files_path = "%s/files" % self.path
        
class F4Loader:
    def __init__(self, endpoint, data_dir = "./data", quietly = False, csv_file = None):
        self.endpoint = endpoint
        self.data_dir = data_dir
        self.csv_file = csv_file
        self.logger = self.create_stream_logger('csv2f4', stdout)
        if quietly:
            self.logger.setLevel(30)


        if csv_file:
            self.csv_data = CSVData(csv_file)
            self.csv_data.get_items()
    
    


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
        """Handle any dowloads, cache as files locally, then upload all files"""
        http = httplib2.Http()
        download_this = True
        files = []
        for url_field in item.URLs:
            url = url_field.value
            filename = urlparse.urlsplit(url).path.split("/")[-1]
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
                url = "%s/objects/%s/files" % (endpoint, item.id)
                headers = {"Content-Type": "text/turtle", "slug": "files"}
                if not requests.get(url).ok : 
                    r = requests.post(url = "%s/objects/%s" % (endpoint, item.id), headers=headers)
                    print "**********", url, r, r.text
                (path, filename) = os.path.split(fyle)
                url = "%s/%s" % (url, filename)
                headers={'Content-Type': 'application/octet-stream'}
                r = requests.put(url = url, data=open(fyle).read(), headers=headers)
                print r, r.text
                self.logger.info("Uploaded %s", fyle)

    def resource_exists(self,type, id):
        url = "%s/%s/%s" % (endpoint, type, id)
        res = requests.get(url)
        return res.ok

    def load(self):
        uploaded_item_ids = []
        collections = {}
        for collection in self.csv_data.collections:
            collection_name = collection.id
            print "Processing potential collection: %s" % collection_name
            data = collection.serialize_RDF()
            headers = {'Content-type': 'text/turtle', 'Slug': collection_name, 'Accept': 'text/turtle'}
            collection_url = "%s/collections/%s" % (endpoint, collection_name)
            res = requests.delete(url = collection_url)
            res = requests.delete(url = collection_url + "/fcr:tombstone")
            res = requests.put(url = collection_url,
                                headers = headers,
                                data = data)
            #print collection_url, res.text



        for item in self.csv_data.items:
            id = item.id

            if id != None:
                title = item.title
                type = item.type
                #TODO
                #previous_id = omeka_client.get_item_id_by_dc_identifier(id)
                item_path = "/rest/objects/%s" % id
                item_url = "%s/objects/%s" % (endpoint, id)
                if item.in_collection:
                    collection_id = item.in_collection

                    #TODO check if it exists
                    collection_path = "/collections/%s" % (collection_id)
                    if self.resource_exists('collections', collection_id):
                        item.graph.add((Literal("<>"), URIRef("http://pcdm.org/models#memberOf"), URIRef("/rest%s" % collection_path)))

                # Deal with relations between objects
                for rel in item.relations:
                    object_id = rel.value
                    object_path = "/rest/objects/%s" % (object_id)
                    #print "Relating"
                    if self.resource_exists('objects', object_id):
                        item.graph.add((Literal("<>"),URIRef(rel.qualified_name), URIRef(object_path)))
                    #TODO _ back-relations?

                # Upload it
                data = item.serialize_RDF()

               
                headers = {'Content-type': 'text/turtle', 'Accept': 'text/turtle'}
                if self.resource_exists('objects',id):
                    print "already here", id
                    data = requests.get(item_url).text + data
                else:
                     print "Minting", id
                     headers['slug'] = id
                res = requests.put(url = item_url,
                                    headers = headers,
                                    data = data)
                #print res, res.text

                if item.in_collection and self.resource_exists("collections", collection_id):
                    headers = {"Content-Type": "text/turtle"}
                    membership = "\n<> <http://pcdm.org/models#hasMember> <%s> \n.\n" % (item_path)
                    collection_url = "%s/%s" % (endpoint, collection_path)
                    new_collection_data = requests.get(collection_url).text + membership
                    res = requests.put(url = collection_url, data = new_collection_data, headers=headers)
                    #print res, res.text


                print item_url
                #print requests.get(item_url).text
                self.download_and_upload_files(item)


   
if __name__ == "__main__":
    # Define and parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('inputfile', type=argparse.FileType('rb'),  default=stdin, help='Name of input Excel file')

    parser.add_argument('-u', '--api_url',default=None, help='Fedora URL')
    parser.add_argument('-d', '--download_cache', default="./data", help='Path to a directory in which to chache dowloads (defaults to ./data)')
    parser.add_argument('-q', '--quietly', action='store_true', help='Only log errors and warnings not the constant stream of info')
    args = vars(parser.parse_args())


    endpoint = args['api_url'] 
    csv_file = args['inputfile']
    quietly = args['quietly']
    data_dir = args['download_cache']

    f4 = F4Loader(endpoint, data_dir, quietly, csv_file)
    f4.load()





