# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 14:55:28 2018

@author: lapotre
"""
from zipfile import ZipFile
from os import remove
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions
import urllib.error
import json

errors = open("errors.txt", "w", encoding="utf-8")

# data = pd.read_csv('/home/lapotre/articles_blog/inriaviz/sampleSPath.csv')
data = pd.read_csv('sampleSPath.csv')
# data = pd.read_csv('test.csv')

sparql = SPARQLWrapper("http://data.bnf.fr/sparql")


def query(entrydict, jointlevelkey, minus, number, uri=""):      
    for i in range(len(pd.DataFrame(entrydict)[jointlevelkey].dropna())):
        if pd.DataFrame(entrydict)[jointlevelkey].dropna()[i]['type'] == 'uri': 
            sparql_query = """
                        SELECT distinct *
                        WHERE
                        {
                        <%s> ?lien%s%s ?level%s.                
                        }               
                    """%(pd.DataFrame(entrydict)[jointlevelkey].dropna()[i]['value'], minus, number, number)
            sparql.setQuery(sparql_query)
           
            sparql.setReturnFormat(JSON)
            try:
                results = sparql.query().convert()
                entrydict[i]['children'] = results['results']['bindings']
                print("query", i, uri, "\n", entrydict[i]['children'], "\n")
            except SPARQLExceptions.EndPointNotFound as err:
                errors.write(sparql_query + "\n" + str(err) + "\n\n")
            except urllib.error.URLError as err:
                errors.write(sparql_query + "\n" + str(err) + "\n\n")
            except urllib.error.HTTPError as err:
                errors.write(sparql_query + "\n" + str(err) + "\n\n")
        else :
            entrydict[i]['children'] = []
        

def uri2extract(uri):
    print(uri)
    local_dict = {}
    sparql.setQuery("""
                    SELECT distinct *
                    WHERE
                    {
                    <%s> ?lien01 ?level1.                
                    }               
                """ %uri)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    local_dict[uri] = results['results']['bindings']
    
    query(local_dict[uri], 'level1', '1', '2', uri)
    
    for e in range(len(local_dict[uri])) :
        if ("children" in local_dict[uri][e]
            and len(local_dict[uri][e]['children']) > 0):
            query(local_dict[uri][e]['children'], 'level2', '2', '3', uri)
            for f in range(len(local_dict[uri][e]['children'])):
                if ("children" in local_dict[uri][e]['children'][f]
                    and
                    len(local_dict[uri][e]['children'][f]['children']) > 0):
                    query(local_dict[uri][e]['children'][f]['children'], 'level3', '3', '4', uri)
                    for g in range(len(local_dict[uri][e]['children'][f]['children'])):
                        if ("children" in local_dict[uri][e]['children'][f]['children'][g]
                            and
                            len(local_dict[uri][e]['children'][f]['children'][g]['children']) > 0):
                            query(local_dict[uri][e]['children'][f]['children'][g]['children'], 'level4', '4', '5', uri)
                            for h in range(len(local_dict[uri][e]['children'][f]['children'][g]['children'])):
                                if ("children" in local_dict[uri][e]['children'][f]['children'][g]['children'][h]
                                    and
                                    len(local_dict[uri][e]['children'][f]['children'][g]['children'][h]['children']) > 0):
                                    query(local_dict[uri][e]['children'][f]['children'][g]['children'][h]['children'], 'level5', '5', '6', uri)
    return local_dict[uri]

def liste2split(data, i):
    report_name = f'sampleSPath{str(i)}.json'
    o = open(report_name, "w", encoding="utf-8")
    dict_report = {}
    max = split_param
    if (len(data) < split_param):
        max = len(data)
    for uri in data['entite'][0:max]:
        print(report_name, uri)
        dict_report[uri] = uri2extract(uri)
    dump = json.dumps(dict_report)
    o.write(dump)
    o.close()
    with ZipFile(f'{report_name[:-5]+".zip"}', 'w', compresslevel=9) as myzip:
        myzip.write(report_name)
    remove(report_name)
    if (len(data) > split_param):
        liste2split(data[split_param:], i+1)
    

split_param = 50

if __name__ == "__main__":
    i = 4
    liste2split(data[150:], i)




#for e in final_dict['http://data.bnf.fr/ark:/12148/cb119086682#about'] :
#    query(final_dict['http://data.bnf.fr/ark:/12148/cb119086682#about'][e]['children'], 'level2', '2', '3')
          