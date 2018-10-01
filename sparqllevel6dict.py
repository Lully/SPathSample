# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 14:55:28 2018

@author: lapotre
"""

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions
import json

errors = open("errors.txt", "w", encoding="utf-8")

#data = pd.read_csv('/home/lapotre/articles_blog/inriaviz/sampleSPath.csv')
data = pd.read_csv('sampleSPath.csv')
#data = pd.read_csv('test.csv')

final_dict = {}
sparql = SPARQLWrapper("http://data.bnf.fr/sparql")


def query(entrydict, jointlevelkey, minus, number, uri=""):
        
    for i in range(len(pd.DataFrame(entrydict)[jointlevelkey].dropna())) :
        if pd.DataFrame(entrydict)[jointlevelkey].dropna()[i]['type'] == 'uri' : 
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
                errors.write(sparql_query + "\n\n")
        else :
            entrydict[i]['children'] = []
        

for uri in data['entite']:
    print(uri)
    sparql.setQuery("""
                    SELECT distinct *
                    WHERE
                    {
                    <%s> ?lien01 ?level1.                
                    }               
                """%uri)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    final_dict[uri] = results['results']['bindings']
    list_df = []
    
    
    query(final_dict[uri], 'level1', '1', '2', uri)
    
    for e in range(len(final_dict[uri])) :
        if ("children" in final_dict[uri][e]
            and len(final_dict[uri][e]['children']) > 0) :
            query(final_dict[uri][e]['children'], 'level2', '2', '3', uri)
            for f in range(len(final_dict[uri][e]['children'])):
                if ("children" in final_dict[uri][e]['children'][f]
                    and
                    len(final_dict[uri][e]['children'][f]['children']) > 0) :
                    query(final_dict[uri][e]['children'][f]['children'], 'level3', '3', '4', uri)
                    for g in range(len(final_dict[uri][e]['children'][f]['children'])):
                        if ("children" in final_dict[uri][e]['children'][f]['children'][g]
                            and
                            len(final_dict[uri][e]['children'][f]['children'][g]['children']) > 0) :
                            query(final_dict[uri][e]['children'][f]['children'][g]['children'], 'level4', '4', '5', uri)
                            for h in range(len(final_dict[uri][e]['children'][f]['children'][g]['children'])):
                                if ("children" in final_dict[uri][e]['children'][f]['children'][g]['children'][h]
                                    and
                                    len(final_dict[uri][e]['children'][f]['children'][g]['children'][h]['children']) > 0) :
                                    query(final_dict[uri][e]['children'][f]['children'][g]['children'][h]['children'], 'level5', '5', '6', uri)


dump = json.dumps(final_dict)
o = open('sampleSPath.json', "w", encoding="utf-8")
o.write(dump)
o.close()

#for e in final_dict['http://data.bnf.fr/ark:/12148/cb119086682#about'] :
#    query(final_dict['http://data.bnf.fr/ark:/12148/cb119086682#about'][e]['children'], 'level2', '2', '3')
          