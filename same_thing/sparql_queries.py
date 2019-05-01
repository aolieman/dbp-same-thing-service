
latest_global_ids = """
    PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX dcat:  <http://www.w3.org/ns/dcat#>
    
    # Get all files
    SELECT DISTINCT ?file ?latest WHERE {
        ?dataset dataid:artifact <https://databus.dbpedia.org/dbpedia/id-management/global-ids> .
        ?dataset dcat:distribution ?distribution .
        ?distribution dataid:contentVariant 'base58'^^<http://www.w3.org/2001/XMLSchema#string> .
        ?distribution dataid:formatExtension 'tsv'^^<http://www.w3.org/2001/XMLSchema#string> .
        ?distribution dataid:compression 'bzip2'^^<http://www.w3.org/2001/XMLSchema#string> .
        ?distribution dcat:downloadURL ?file .
        {SELECT ?dataset ?latest WHERE { # join with latest version available
                ?dataset dataid:artifact <https://databus.dbpedia.org/dbpedia/id-management/global-ids> .
                ?dataset dcat:distribution ?distribution .
                ?distribution dataid:contentVariant 'base58'^^<http://www.w3.org/2001/XMLSchema#string> .
                ?distribution dataid:formatExtension 'tsv'^^<http://www.w3.org/2001/XMLSchema#string> .
                ?distribution dataid:compression 'bzip2'^^<http://www.w3.org/2001/XMLSchema#string> .
                ?dataset dct:hasVersion ?latest .
            } ORDER BY DESC(?latest) LIMIT 1 
      }
    }
"""
