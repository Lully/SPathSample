# coding: utf-8

from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions
import urllib.error, urllib.request, urllib.parse
import re
from pprint import pprint

manifs_traitees = []

def uri2manifs(uri, report):
    query = """
PREFIX dcterms: <http://purl.org/dc/terms/>
CONSTRUCT {?manif ?prop ?val} where {
  ?manif dcterms:subject <""" + uri + """>.
  ?manif ?prop ?val.
}
    """
    url = "http://data.bnf.fr/sparql?default-graph-uri=&query=" + urllib.parse.quote(query) + "&format=text%2Fplain&timeout=0&should-sponge=&debug=on"
    try:
        page = urllib.request.urlopen(url).read().decode(encoding="utf-8")
        
        page = page.split("\n")
        liste_manifs_page = []
        for line in page:
            if (line):
                manif = line.split("<")[1].split(">")[0]
                if (manif not in manifs_traitees):
                    liste_manifs_page.append(manif)
                    report.write(line + "\n")
        for manif in liste_manifs_page:
            manifs_traitees.append(manif)
    except urllib.error.URLError as err:
        pass


if __name__ == "__main__":
    filename = input("Nom du fichier contenant les URI Rameau : ")
    if not filename:
        filename = "urirameau.txt"
    reportname = filename[:-4] + "-manifs.txt"
    report = open(reportname, "w", encoding="utf-8")
    with open(filename) as f:
        for row in f:
            uri = row.replace("\n", "")
            print(uri)
            uri2manifs(uri, report) 
