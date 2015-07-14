#!/usr/bin/python
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

sys.path.append("../omeka-python-utils")
from csv2repo import CSVData, Field, Item, Namespace






""" Uploads an entire spreadsheet to a Fedora server

This code was adapted from 

"""



# Define and parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument('inputfile', type=argparse.FileType('rb'),  default=stdin, help='Name of input Excel file')

parser.add_argument('-u', '--api_url',default=None, help='Fedora URL')
parser.add_argument('-d', '--download_cache', default="./data", help='Path to a directory in which to chache dowloads (defaults to ./data)')
parser.add_argument('-c', '--create_collections', action='store_true', help='Auto-create modelsissing collections')
parser.add_argument('-q', '--quietly', action='store_true', help='Only log errors and warnings not the constant stream of info')
args = vars(parser.parse_args())


endpoint = args['api_url'] 

inputfile = args['inputfile']

data_dir = args['download_cache']
if args["quietly"]:
    logger.setLevel(30)



csv_data = CSVData(inputfile)
csv_data.get_items()


def resource_exists(type, id):
    url = "%s/%s/%s" % (endpoint, type, id)
    res = requests.get(url)
    return res.ok

uploaded_item_ids = []
collections = {}

for collection in csv_data.collections:
    collection_name = collection.id
    print "Processing potential collection: %s" % collection_name
    data = collection.serialize_RDF()
    headers = {'Content-type': 'text/turtle', 'Slug': collection_name, 'Accept': 'text/turtle'}
    collection_url = "%s/collections/%s" % (endpoint, collection_name)
    # Delete first - heavy handed I know
    print data
    res = requests.delete(url = collection_url)
    res = requests.delete(url = collection_url + "/fcr:tombstone")
    res = requests.put(url = collection_url,
                        headers = headers,
                        data = data)
    print collection_url, res.text
    


for item in csv_data.items:
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
            if resource_exists('collections', collection_id):
                item.graph.add((Literal("<>"), URIRef("http://pcdm.org/models#memberOf"), URIRef("/rest%s" % collection_path)))
                
        # Deal with relations between objects
        for rel in item.relations:
            object_id = rel.value
            object_path = "/rest/objects/%s" % (object_id)
            #print "Relating"
            if resource_exists('objects', object_id):
                #print "Relation to ", object_path
                item.graph.add((Literal("<>"),URIRef(rel.qualified_name), URIRef(object_path)))
            #TODO _ back-relations
            
        # Upload it
        data = item.serialize_RDF()
        #print data
         
        headers = {'Content-type': 'text/turtle', 'Accept': 'text/turtle'}
        if resource_exists('objects',id):
            print "already here", id
            data = requests.get(item_url).text + data
        else:
             print "Minting", id
             headers['slug'] = id
        res = requests.put(url = item_url,
                            headers = headers,
                            data = data)
        print res, res.text
           
        if item.in_collection and resource_exists("collections", collection_id):
            headers = {"Content-Type": "text/turtle"}
            membership = "\n<> <http://pcdm.org/models#hasMember> <%s> \n.\n" % (item_path)
            collection_url = "%s/%s" % (endpoint, collection_path)
            new_collection_data = requests.get(collection_url).text + membership
            res = requests.put(url = collection_url, data = new_collection_data, headers=headers)
            print res, res.text


        print item_url
        print requests.get(item_url).text

                
                
   


