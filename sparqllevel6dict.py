# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 14:55:28 2018

@author: lapotre
"""
from zipfile import ZipFile
from os import remove
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions
import urllib.error, urllib.request
from rdflib.graph import Graph
import rdflib
from lxml import html
import json

errors = open("errors2.txt", "w", encoding="utf-8")

# data = pd.read_csv('/home/lapotre/articles_blog/inriaviz/sampleSPath.csv')
all_data = pd.read_csv('sampleSPath.csv')
 
sparql = SPARQLWrapper("http://data.bnf.fr/sparql")

# Liste des URI déjà traitées
treated_entities = []


def uri2url_nt(uri):
    """
    Ouvre l'URI et suit la redirection pour avoir l'URL de la page
    afin de pouvoir construire l'URL du fichier .nt
    """
    request = html.parse(urllib.request.urlopen(uri))
    try:
        url_nt = request.find("//a[@id='download-rdf-nt']").get("href")
    except AttributeError:
        url_nt = None
    return url_nt


def url_nt2report(url_nt, report, level):
    print(" "*level, report.name, url_nt, level)
    g = Graph()
    try:
        g.parse(urllib.request.urlopen(url_nt), format="nt")
        objects = []
        for subj, predicate, obj in g:
            # print(subj, predicate, obj)
            subj_str = "<" + str(subj) + ">"
            predicate_str = "<" + str(predicate) + ">"
            obj_str = str(obj)
            if (type(obj) is rdflib.term.Literal):
                obj_str = '"' + obj_str + '"'
            elif (type(obj) is rdflib.term.URIRef):
                if ("data.bnf.fr" in obj_str
                    and obj_str not in treated_entities):
                    objects.append(obj_str)
                    treated_entities.append(obj_str)
                obj_str = "<" + obj_str + ">"
            report.write(" ".join([subj_str, predicate_str, obj_str]) + "\n")
        if (level < 7):
            for object in objects:
                url_nt_object = uri2url_nt(object)
                if (url_nt_object is not None):
                    url_nt2report(url_nt_object, report, level+1)
    except urllib.error.HTTPError as err:
        errors.write(url_nt + "\n" + str(err) + "\n\n")

def liste2split(data, i):
    j = 0
    report_name = f'sampleSPath{str(i)}.nt'
    report = open(report_name, "w", encoding="utf-8")
    max = split_param
    if (len(data) < split_param):
        max = len(data)
    for uri in data['entite'][0:max]:
        j += 1
        print(j, uri)
        if (uri not in treated_entities):
            # print(report_name, uri)
            treated_entities.append(uri)
            url_nt = uri2url_nt(uri)
            if (url_nt is not None):
                url_nt2report(url_nt, report, 1)
    report.close()
    with ZipFile(f'{report_name[:-5]+".zip"}', 'w') as myzip:
        myzip.write(report_name)
    remove(report_name)
    if (len(data) > split_param):
        liste2split(data[split_param:], i+1)

split_param = 50

if __name__ == "__main__":
    i = 1
    liste2split(all_data, i)




#for e in final_dict['http://data.bnf.fr/ark:/12148/cb119086682#about'] :
#    query(final_dict['http://data.bnf.fr/ark:/12148/cb119086682#about'][e]['children'], 'level2', '2', '3')
          