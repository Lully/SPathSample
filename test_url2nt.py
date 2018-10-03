# coding: utf-8

import urllib.request
from lxml import html, etree
from rdflib.graph import Graph
import rdflib

# Liste des URI déjà traitées
treated_entities = []

url = "http://data.bnf.fr/ark:/12148/cb12138677d#about"
request = html.parse(urllib.request.urlopen(url))
url_ref = request.find("//meta[@property='og:url']").get("content")
url_nt = url_ref + "rdf.nt"
url_nt = url_nt.replace("/fr/", "/")
print(url_nt)


def uri2url_nt(uri):
    """
    Ouvre l'URI et suit la redirection pour avoir l'URL de la page
    afin de pouvoir construire l'URL du fichier .nt
    """
    request = html.parse(urllib.request.urlopen(uri))
    url_ref = request.find("//meta[@property='og:url']").get("content")
    url_nt = url_ref + "rdf.nt"
    url_nt = url_nt.replace("/fr/", "/")
    return url_nt


def url_nt2report(url_nt, report, level):
    g = Graph()
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
            if (obj_str not in treated_entities):
                objects.append(obj_str)
                treated_entities.append(obj_str)
            obj_str = "<" + obj_str + ">"
        report.write(" ".join([subj_str, predicate_str, obj_str]) + "\n")
    if (level < 7):
        for object in objects:
            print("object", object)
            url_nt_object = uri2url_nt(object)
            url_nt2report(url_nt_object, report, level+1)

report = open("report_test.txt", "w", encoding="utf-8")
url_nt2report("http://data.bnf.fr/12138677/bengt_emil_johnson/rdf.nt",
        report, 5
        )
#print(treated_entities)