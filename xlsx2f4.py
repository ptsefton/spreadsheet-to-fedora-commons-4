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






""" Uploads an entire spreadsheet to a Fedora server """



# Define and parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument('inputfile', type=argparse.FileType('rb'),  default=stdin, help='Name of input Excel file')

parser.add_argument('-u', '--api_url',default=None, help='Omeka API Endpoint URL (hint, ends in /api)')
parser.add_argument('-d', '--download_cache', default="./data", help='Path to a directory in which to chache dowloads (defaults to ./data)')
parser.add_argument('-c', '--create_collections', action='store_true', help='Auto-create missing collections')
parser.add_argument('-q', '--quietly', action='store_true', help='Only log errors and warnings not the constant stream of info')
args = vars(parser.parse_args())


endpoint = args['api_url'] 

inputfile = args['inputfile']

data_dir = args['download_cache']
if args["quietly"]:
    logger.setLevel(30)


#Auto-map to elements from these sets
#TODO make the 'bespoke' one configurable
default_element_set_names = ['Dublin Core','Item Type Metadata', 'Bespoke Metadata']


def download_and_upload_files(new_item_id, original_id, URLs, files):
    """Handle any dowloads, cache as files, then upload all files"""
    for url in URLs:
        http = httplib2.Http()
        file_path = mapping.downloaded_file(url)
        download_this = True

        print "Found something to download and re-upload %s" % url

        if file_path == None or file_path == "None": #Previous bug put "None" in spreadsheet
            filename = urlparse.urlsplit(url).path.split("/")[-1]
            new_path = os.path.join(data_dir, str(original_id))
            if not os.path.exists(new_path):
                os.makedirs(new_path)
            file_path = os.path.join(new_path, filename)
        logger.info("Local filename: %s", file_path)

        #Check if we have one the same size already
        if os.path.exists(file_path):
            response, content = http.request(url, "HEAD")
            download_size = int(response['content-length']) if 'content-length' in response else -1
            file_size = os.path.getsize(file_path)
            if download_size == file_size:
                logger.info("Already have a download of the same size: %d", file_size)
                download_this = False

        if download_this:
            try:
                response, content = http.request(url, "GET")
                open(file_path,'wb').write(content)
                logger.info(response)
            except:
                logger.warning("Some kind of download error happened fetching %s - pressing on" % url)

        files.append(file_path)
        mapping.add_downloaded_file(url, file_path)

    for fyle in files:
        logger.info("Uploading %s", fyle)
        try:
            omeka_client.post_file_from_filename(fyle, new_item_id )
            
            logger.info("Uploaded %s", fyle)
        except:
            logger.warning("Some kind of error happened uploading %s - pressing on" % fyle)

def upload(previous_id, original_id, jsonstr, title, URLs, files, iterations):
    #TODO - get rid of the global mapping variable 
    if iterations > 1:
        previous_id = None
        
    for iteration in range(0, iterations):
        if previous_id <> None:
            logger.info("Re-uploading %s", previous_id)
            response, content = omeka_client.put("items" , previous_id, jsonstr)
        else:
            logger.info("Uploading new version, iteration %d", iteration)
            response, content = omeka_client.post("items", jsonstr)

        #Looks like the ID wasn't actually there, so get it to mint a new one
        if response['status'] == '404':
            logger.info("retrying")
            response, content = omeka_client.post("items", jsonstr)

        new_item = json.loads(content)

        try:
            new_item_id = new_item['id']
            if iterations == 1:
                id_mapping.append({'Omeka ID': new_item_id, identifier_column: original_id, "Title": title})
            
            logger.info("New ID %s", new_item_id)
            
            for (property_id, object_id) in relations:
                omeka_client.addItemRelation(new_item_id, property_id, object_id)

            download_and_upload_files(new_item_id, original_id, URLs, files)
        except:
            logger.error('********* FAILED TO UPLOAD: \n%s\n%s\n%s', item_to_upload, response, content)




       


#Get the main data
databook = tablib.import_book(inputfile)
data = yaml.load(databook.yaml)


ns_prefixes = ["dc", "foaf"]
blank_record = """
@prefix dc: <http://purl.org/dc/terms/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
<> """

for d in data:
    collection_name =  "XX%s" % d['title']
    print "Processing potential collection: %s" % collection_name
    data = """@prefix dc: <http://purl.org/dc/terms/> .\n@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n \n<> dc:title '%s';""" % collection_name
    headers = {'Content-type': 'text/turtle', 'Slug': collection_name, 'Accept': 'text/turtle'}
    collection_url = "%s/%s" % (endpoint, collection_name)
    # Delete first - heavy handed I know
    res = requests.delete(url = collection_url)
    res = requests.delete(url = collection_url + "/fcr:tombstone")
    res = requests.put(url = collection_url,
                        headers = headers,
                        data = data)
    print collection_url, res.text
    
    #collection_id = omeka_client.getCollectionId(collection_name, create=args['create_collections'], public=args["public"])
    
  
        
      
            
    for key in d['data'][0]:
        print "Column header: %s" % key

    for item in d['data']:
            stuff_to_upload = []
        ##     relations = []
        ##     element_texts = []
        ##     URLs = []
        ##     files = []
            id = None
            for key,value in item.items():
                parts = key.split(":")
                if len(parts) == 2:
                    prefix = parts[0]
                    if prefix in ns_prefixes:
                        stuff_to_upload.append( '  %s   "%s"\n' % (key, value))
                elif key == "ID":
                    id = "YY__%s" % value
                
            if stuff_to_upload != [] and id != None:
                data = "%s %s ." % ( blank_record , ";\n".join(stuff_to_upload))
                item_url = "%s/%s" % (collection_url, id)
                headers = {'Content-type': 'text/turtle', 'Slug': id, 'Accept': 'text/turtle'}
                res = requests.delete(url = item_url)
                res = requests.delete(url = item_url + "/fcr:tombstone")
                res = requests.put(url = item_url,
                headers = headers,
                data = data)
                    
                print item_url, res.text
        ##         (property_id, object_id) = mapping.item_relation(collection_name, key, value)
        ##         if value <> None:
        ##             if key == "Omeka Type":
        ##                 item_type_id = omeka_client.getItemTypeId(value, create=args['create_item_types'])
        ##                 if item_type_id <> None:
        ##                     stuff_to_upload = True
        ##             else:
        ##                 if mapping.has_map(collection_name, key):
        ##                     if  mapping.collection_field_mapping[collection_name][key] <> None:
        ##                         element_text = {"html": False, "text": "none"} #, "element_set": {"id": 0}}
        ##                         element_text["element"] = {"id": mapping.collection_field_mapping[collection_name][key] }
        ##                     else:
        ##                         element_text = {}
                            
        ##                     if mapping.is_linked_field(collection_name, key, value):
        ##                         #TODO - deal with muliple values
        ##                         to_title =  mapping.id_to_title[value]
        ##                         if to_title == None:
        ##                             to_title =  mapping.id_to_omeka_id[value]
        ##                         element_text["text"] = "<a href='/items/show/%s'>%s</a>" % (mapping.id_to_omeka_id[value], to_title)
        ##                         element_text["html"] = True
        ##                         logger.info("Uploading HTML %s, %s, %s", key, value, element_text["text"])
        ##                     elif property_id <> None:
        ##                         logger.info("Relating this item to another")
        ##                         relations.append((property_id, object_id))
        ##                     else:
        ##                         try: # Have had some encoding problems - not sure if this is still needed
        ##                             element_text["text"] = unicode(value)

        ##                         except:
        ##                             logger.error("failed to add this string \n********\n %s \n*********\n" % value)

        ##                     element_texts.append(element_text)
               
        ##         else:
        ##             item[key] = ""

        ##         if mapping.to_download(collection_name, key):
        ##             URLs.append(value)
        ##         if mapping.is_file(collection_name, key) and value:
        ##             filename = os.path.join(data_dir,value)
        ##             if os.path.exists(filename):
        ##                 files.append(filename)    
        ##             else:
        ##                 logger.warning("skipping non existent file %s" % filename)
        ##     if not(identifier_column) in item:
        ##         stuff_to_upload = False
        ##         logger.info("No identifier (%s) in table", identifier_column)
                
        ##     if stuff_to_upload:
        ##         item_to_upload = {"collection": {"id": collection_id}, "item_type": {"id":item_type_id}, "featured": args["featured"], "public": args["public"]}
        ##         item_to_upload["element_texts"] = element_texts
        ##         jsonstr = json.dumps(item_to_upload)
        ##         previous_id = None
        ##         original_id = item[identifier_column]
        ##         title = item[title_column] if title_column in item else "Untitled"
        ##         if identifier_column in item and original_id in mapping.id_to_omeka_id:
        ##             previous_id = mapping.id_to_omeka_id[original_id]

                
        ##         upload(previous_id, original_id, jsonstr, title, URLs, files, iterations)
                

                
                
   


