from re import sub
from numpy import number
from json import load
from pandas import read_csv
from pandas import Series
from pandas import DataFrame
from pandas import merge
from rdflib import Graph
from rdflib import URIRef
from rdflib import Literal
from rdflib import RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

class TriplestoreProcessor(object):
    def __init__(self):
        self.endpointUrl = ""

    def getEndpointUrl(self):
        return self.endpointUrl
    
    def setEndpointUrl(self, endpointUrl):
        self.endpointUrl = endpointUrl
        return isinstance(endpointUrl, str)

class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self) -> None: #return annotation because Triplestore data processor doesn't return anything but the boolean value and the populated FF_graph through its method uploadData.
        super().__init__()

    def uploadData(self, path):
        # input: user provided path of a csv or json file.
        # output: a boolean value.
        # Once instanciated all the triples in the FF_graph, the function uploadData also provides the updating of the SPARQL store into the BlazegraphDB.
        
        # === STEP 1: INSTANTIATE THE ENDPOINT AND OPEN-CLOSE THE CONNECTION TO THE STORE === #
        # Firstly, I want to get the endpoint, in order to open the connection to the BlazegraphDB and explore it.
        # Secondly, I want to keep track of the "current situation" of the database before storing in it new triples, e.g. if there are already existent triples in it or not (this is done in the case the function uploadData is called multiple times).
        # Finally, I can close the connection to create new triples.
        endpoint = self.getEndpointUrl()
        store = SPARQLUpdateStore()
        store.open((endpoint, endpoint))
        storedTriples = store.__len__(context=None)
        store.close()

        # === STEP 2: CREATE THE RDF GRAPH AND POPULATE IT === #
        FF_graph = Graph()

        # classes 
        JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
        BookChapter = URIRef("https://schema.org/Chapter")
        Journal = URIRef("https://schema.org/Periodical")
        Book = URIRef("https://schema.org/Book")
        Proceedings = URIRef("https://schema.org/EventSeries")
        ProceedingsPaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")

        # attributes related to classes
        doi = URIRef("https://schema.org/identifier") #used for every identifier
        publicationYear = URIRef("https://dbpedia.org/ontology/year")
        title = URIRef("https://schema.org/name")
        issue = URIRef("https://schema.org/issueNumber")
        volume = URIRef("https://schema.org/volumeNumber")
        chapter_num = URIRef("https://schema.org/numberedPosition")
        publisher = URIRef("https://schema.org/publisher")
        publicationVenue = URIRef("https://schema.org/isPartOf")
        author = URIRef("https://schema.org/author")
        name = URIRef("https://schema.org/givenName")
        surname = URIRef("https://schema.org/familyName")
        citation = URIRef("https://schema.org/citation")
        event = URIRef("https://schema.org/event")

        base_url = "https://FF.github.io/res/"
        
        if ".csv" in path: # <<<<<<< CSV FILE FROM PATH >>>>>>>>
            publicationVenue_cache = {} #caches here are counters in the form of dictionaries
            publicationPublisher_cache = {}
            publicationVenueIdx_cache = 0 #counter
            publicationCounter = 0 #I need a publicationCounter to know how many publications are in my Store. See the documentation for more.
            venueProceedings_cache = {}  #cache for proceedings. 
            publications = read_csv (path, keep_default_na=False,
                                    dtype={
                                        "id": "string",
                                        "title": "string",
                                        "type":"string",
                                        "publication_year":"int",
                                        "issue": "string",
                                        "volume": "string",
                                        "chapter":"string",
                                        "publication_venue": "string",
                                        "venue_type": "string",
                                        "publisher":"string",
                                        "event":"string"
                                        })
            if storedTriples == 0: #if my SPARQL store is empty [FIRST CASE SCENARIO]
                # --- PUBLICATIONS --- #
                for idx, row in publications.iterrows():
                    publication_id = "publication-" + str(idx) #instantiate the first node to build the triples
                    subj = URIRef(base_url + publication_id) #publication as subject
                    # basic publication triples:
                    FF_graph.add((subj, doi, Literal(row["id"])))
                    FF_graph.add((subj, title, Literal(row["title"])))
                    FF_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    #triples for Journal Article class:
                    if row["type"] == "journal-article":
                        FF_graph.add((subj, RDF.type, JournalArticle))
                        FF_graph.add((subj, issue, Literal(row["issue"]))) #when I'll query the database, I'll ask for Publication of type "" that has issue/volume
                        FF_graph.add((subj, volume, Literal(row["volume"]))) 
                    #triples for Book Chapter class:
                    elif row["type"] == "book-chapter":
                        FF_graph.add((subj, RDF.type, BookChapter))
                        FF_graph.add((subj, chapter_num, Literal(row["chapter"])))
                    elif row["type"] == "proceedings-paper":
                        FF_graph.add((subj, RDF.type, ProceedingsPaper))
                    # --- VENUES --- I check whether the Venues are present or not in my cache 
                    publicationVenueValue = row["publication_venue"]
                    if publicationVenueValue not in publicationVenue_cache:
                        venues_id = "venue-" + str(len(publicationVenue_cache)) # since the store is empty, the first venuesubject-related triple I'll add will be the first in absolute.
                        subjVenue = URIRef(base_url + venues_id)  #instantiate the subject node
                        publicationVenue_cache[publicationVenueValue] = subjVenue #the value is now part of my cache
                        #Publication-hasVenue-Venue triples:
                        FF_graph.add((subj, publicationVenue, subjVenue))
                        venueTypeValue = row["venue_type"]
                        #triples for Journal class:
                        if venueTypeValue == "journal":
                            FF_graph.add((subjVenue, RDF.type, Journal))
                        #triples for Book class:
                        elif venueTypeValue == "book":
                            FF_graph.add((subjVenue, RDF.type, Book))
                        #triple for Proceedings class:
                        elif venueTypeValue == "proceedings":
                            FF_graph.add((subjVenue, RDF.type, Proceedings))
                            FF_graph.add((subjVenue, event, Literal(row["event"])))
                        #triple for Publication Venue Value:
                        FF_graph.add((subjVenue, title, Literal(publicationVenueValue)))  
                    elif publicationVenueValue in publicationVenue_cache:
                        subjVenue = publicationVenue_cache[publicationVenueValue]
                        FF_graph.add((subj, publicationVenue, subjVenue))

                    publisherValue = row["publisher"]
                    if publisherValue not in publicationPublisher_cache:
                        subjPublisher = URIRef(base_url + "publisher-" + str(len(publicationPublisher_cache)))
                        publicationPublisher_cache[publisherValue] = subjPublisher #the value is now part of my cache
                        #Publisher related triples -> (Publication-hasPublisher-Publisher(Name)) | (Publisher-isPublisherOf, PublicationDoi) 
                        FF_graph.add((subj, publisher, subjPublisher))
                        FF_graph.add((subjPublisher, doi, Literal(publisherValue)))
                    else:
                        subjPublisher = publicationPublisher_cache[publisherValue]
                        FF_graph.add((subj, publisher, subjPublisher)) # publication has Publisher(Name)

            elif storedTriples > 0: #if my SPARQL store has been already set with some triples, I want to track what element and how many elements are there in it.
                pubQuery = """
                PREFIX schema: <https://schema.org/>
                SELECT ?publication
                WHERE {
                ?publication schema:identifier ?identifier .
                FILTER regex(?identifier, "doi")
                }
                """
                publicationOutput = get(endpoint, pubQuery, True)
                numberOfPublication = publicationOutput.shape[0]

                venQuery ="""
                PREFIX schema: <https://schema.org/>
                SELECT DISTINCT ?venue
                WHERE {
                ?publ schema:isPartOf ?venue .
                }
                """
                publicationVenueIdx_cache = get(endpoint, venQuery, True).shape[0] 
                for idx, row in publications.iterrows():
                    publicationDoiValues = row["id"] #value present in the dataframe, to search in the existing store
                    comparePublications_query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" . 
                    }}
                    """
                    comparePublicationsOutput = get(endpoint, comparePublications_query.format(publicationDoiValues), True)
                    # --- PUBLICATIONS --- #
                    if comparePublicationsOutput.empty: #if there are no doi stored in the output dataframe from the previous query, i.e. if that doi value is not in anytriple as subject:
                        subj = URIRef(base_url + "publication-" + str(numberOfPublication + publicationCounter))
                        publicationCounter +=1
                        FF_graph.add((subj, doi, Literal(publicationDoiValues)))
                        # --- VENUES --- After querying about dois, now in the same condition loop I insert another query, to complete the framework "publication-has-publication-venue". 
                        # I expect that there will be no venue value stored yet, unless it comes from another csv file uploading operation.
                        venuQuery ="""
                        PREFIX schema: <https://schema.org/>
                        SELECT ?venue
                        WHERE {{
                            ?publication schema:isPartOf ?venue .
                            ?venue schema:name "{0}" .
                        }}
                        """ 
                        comparePublicationVenuesOutput = get(endpoint, venuQuery.format(row["publication_venue"]), True)
                        if comparePublicationVenuesOutput.empty: #no venues shared in common
                            if row["publication_venue"] in publicationVenue_cache: # I check also the publication Venue cache
                                subjVenue = publicationVenue_cache[row["publication_venue"]] 
                            else:
                                subjVenue = URIRef(base_url + "venue-" + str(publicationVenueIdx_cache + len(publicationVenue_cache)))
                                publicationVenue_cache[row["publication_venue"]] = subjVenue
                                #Journal Class-related triple
                                if row["venue_type"] == "journal": 
                                    FF_graph.add((subjVenue, RDF.type, Journal))
                                #Book Class-related triples
                                elif row["venue_type"] == "book":
                                    FF_graph.add((subjVenue, RDF.type, Book))
                                #Proceedings Class-related triples
                                elif row["venue_type"] == "proceedings":
                                    FF_graph.add((subjVenue, RDF.type, Proceedings))
                                    FF_graph.add((subjVenue, event, Literal(row["event"])))
                                FF_graph.add((subjVenue, title, Literal(row["publication_venue"])))
                            
                        else: #if the doi value is already stored in the graph:
                            subjVenue = URIRef(comparePublicationVenuesOutput.at[0, "venue"]) 
                            if row["venue_type"] == "journal": 
                                FF_graph.add((subjVenue, RDF.type, Journal))
                            elif row["venue_type"] == "book":
                                FF_graph.add((subjVenue, RDF.type, Book))
                            elif row["venue_type"] == "proceedings":
                                FF_graph.add((subjVenue, RDF.type, Proceedings))
                                FF_graph.add((subjVenue, event, Literal(row["event"])))
                            FF_graph.add((subjVenue, title, Literal(row["publication_venue"])))
                        FF_graph.add((subj, publicationVenue, subjVenue))

                    else: #if I have already some publications in the graph registered as subject [SECOND CASE SCENARIO]
                        publicationQuery = """
                        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX schema: <https://schema.org/>

                        SELECT ?publication
                        WHERE {{
                            ?publication schema:identifier "{0}"
                        }}
                        """
                        subj = URIRef(get(endpoint, publicationQuery.format(publicationDoiValues), True).at[0, 'publication'])
                        #I check if a doi is already linked to a venue
                        venueQuery ="""
                        PREFIX schema: <https://schema.org/>
                        SELECT ?venue
                        WHERE {{
                            ?publication schema:isPartOf ?venue .
                            ?publication schema:identifier "{0}" .
                        }}
                        """
                        comparePublicationAndVenues = get(endpoint, venueQuery.format(publicationDoiValues), True)
                        if comparePublicationAndVenues.empty:
                            if row["publication_venue"] in publicationVenue_cache:
                                subjVenue = publicationVenue_cache[row["publication_venue"]]
                            else:
                                venueQuery ="""
                                PREFIX schema: <https://schema.org/>
                                SELECT ?venue
                                WHERE {{
                                    ?publication schema:isPartOf ?venue .
                                    ?venue schema:name "{0}" .
                                }}
                                """
                                compareVenues = get(endpoint, venue_query.format(row["publication_venue"]), True)
                                if compareVenues.empty:
                                    subjVenue = URIRef(base_url + "venue-" + str(publicationVenueIdx_cache + len(publicationVenue_cache)))
                                    publicationVenue_cache[row["publication_venue"]] = subjVenue
                                    if row["venue_type"] == "journal": 
                                        FF_graph.add((subjVenue, RDF.type, Journal))
                                    elif row["venue_type"] == "book":
                                        FF_graph.add((subjVenue, RDF.type, Book))
                                    elif row["venue_type"] == "proceedings":
                                        FF_graph.add((subjVenue, RDF.type, Proceedings))
                                        FF_graph.add((subjVenue, event, Literal(row["event"])))

                                    FF_graph.add((subjVenue, title, Literal(row["publication_venue"])))
                                else:
                                    subjVenue = URIRef(compareVenues.at[0, 'venue'])      
                        else: 
                            subjVenue = URIRef(comparePublicationAndVenues.at[0, 'venue'])

                        if row["venue_type"] == "journal": 
                            FF_graph.add((subjVenue, RDF.type, Journal))
                        elif row["venue_type"] == "book":
                            FF_graph.add((subjVenue, RDF.type, Book))
                        elif row["venue_type"] == "proceedings":
                            FF_graph.add((subjVenue, RDF.type, Proceedings))
                            FF_graph.add((subjVenue, event, Literal(row["event"])))
                        FF_graph.add((subjVenue, title, Literal(row["publication_venue"])))
                        FF_graph.add((subj, publicationVenue, subjVenue))
                        
                    if row["type"] == "journal-article":
                        FF_graph.add((subj, RDF.type, JournalArticle))
                        FF_graph.add((subj, issue, Literal(row["issue"]))) 
                        FF_graph.add((subj, volume, Literal(row["volume"]))) 
                    elif row["type"] == "book-chapter":
                        FF_graph.add((subj, RDF.type, BookChapter))
                        FF_graph.add((subj, chapter_num, Literal(row["chapter"])))
                    elif row["type"] == "proceedings-paper":
                        FF_graph.add((subj, RDF.type, ProceedingsPaper))
                    FF_graph.add((subj, title, Literal(row["title"])))
                    FF_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    
                    comparePublishers_query="""
                    PREFIX schema:<https://schema.org/>
                    SELECT ?publisher
                    WHERE {{
                        ?publisher schema:identifier "{0}"
                    }}
                    """
                    comparePublishersOutput = get(endpoint, comparePublishers_query.format(row["publisher"]), True)
                    if comparePublishersOutput.empty:
                        if row["publisher"] in publicationPublisher_cache:
                            subjPublisher = publicationPublisher_cache[row["publisher"]]
                            FF_graph.add((subj, publisher, subjPublisher))
                        else:
                            publi_query = """
                            PREFIX schema:<https://schema.org/>
                            SELECT ?publisher
                            WHERE {
                                ?publisher schema:identifier ?pub_id .
                                FILTER regex(?pub_id, "crossref")
                            }
                            """
                            publiOutput = get(endpoint, publi_query, True).shape[0]
                            subjPublisher = URIRef(base_url + "publisher-" + str(len(publicationPublisher_cache) + publiOutput))
                            FF_graph.add((subj, publisher, subjPublisher))
                            FF_graph.add((subjPublisher, doi, Literal(row["publisher"])))
                            publicationPublisher_cache[row["publisher"]] = subjPublisher
                    else:
                        subjPublisher = URIRef(comparePublishersOutput.at[0, "publisher"])
                        FF_graph.add((subj, publisher, subjPublisher))
        
        elif ".json" in path: # <<<<<<< JSON FILE FROM PATH >>>>>>>> 
            with open(path, "r", encoding="utf-8") as file:
                jsondata = load(file) #since json is a hierarchical datatype, I can see it as a dictionary of dictionaries.

            authors = jsondata.get("authors") #json object
            venues = jsondata.get("venues_id") #json object
            references = jsondata.get("references") #json object
            publishers = jsondata.get("publishers") #json object

            #See the documentation for more about empty dictionaries and empty lists instantiated here.
            doi_authors=[]
            family=[]
            given=[]
            orcid=[]
            for key in authors:
                a=[] #list for family value
                b=[] #list for given value
                c=[] #list for orcid value
                authorValue = authors[key] #doi related
                doi_authors.append(key)
                for dict in authorValue: #values of the author
                    a.append(dict["family"]) 
                    b.append(dict["given"]) 
                    c.append(dict["orcid"]) 
                family.append(a) 
                given.append(b)
                orcid.append(c)

                fromDoiAuthors_s=Series(doi_authors) 
            fromFamily_s=Series(family)
            fromGiven_s=Series(given)
            fromOrcid_s=Series(orcid)
            # === AUTHORS DATAFRAME POPULATED:
            authors_df=DataFrame({
                "auth doi" : fromDoiAuthors_s,
                "family name" : fromFamily_s, 
                "given name" : fromGiven_s, 
                "orcid" : fromOrcid_s
            })

            doi_venues_id=[] #a list for venues-related doi. 
            issn_isbn=[]
            for key in venues: #I iterate over the venues json object.
                venuesValue = venues[key]
                doi_venues_id.append(key)
                issn_isbn.append(venuesValue)
            standaloneVenues=[] #list of issn
            doi_Cache=[] #list of doi issn/ibn-related
            for a in range(len(issn_isbn)): #here I iterate over the range of the lenght of the list, to obtain a unique index.
                if issn_isbn[a] not in standaloneVenues: #if the issn/isbn value is not present in the list of venues, add it in the form of a new key.
                    standaloneVenues.append(issn_isbn[a])
                    dois = [] 
                    dois.append(doi_venues_id[a])
                    doi_Cache.append(dois)
                else:
                    b = standaloneVenues.index(issn_isbn[a])
                    doi_Cache[b].append(doi_venues_id[a])

            standaloneDoiVenues_s=Series(doi_Cache) #standalone series
            standaloneIssn_s=Series(standaloneVenues)
            # === VENUES DATAFRAME POPULATED:
            standaloneVenues_df=DataFrame({
                "venues doi" : standaloneDoiVenues_s,
                "issn" : standaloneIssn_s, 
            })
            venuesValue_df = standaloneVenues_df[["venues doi", "issn"]]
            venues_Id = []
            for idx, row in venuesValue_df.iterrows():
                venues_Id.append("venues-" + str(idx))
            venuesValue_df.insert(0, "venues id", Series(venues_Id, dtype="string"))
            venuesIdValue_s=venuesValue_df.filter(["venues id"]) # I extract the data
            venuesIdValue_s=venuesIdValue_s.values.tolist() # Transform it into a list
            doi_venues_id=venuesValue_df.filter(["venues doi"])
            doi_venues_id=doi_venues_id.values.tolist()
            venues_id_issn=venuesValue_df.filter(["issn"])
            venues_id_issn=venues_id_issn.values.tolist()

            internalId=[]
            doiVenues=[]
            issnVenues=[]
            for dv in doi_venues_id: #iterating over every series
                e=doi_venues_id.index(dv)
                for list in dv:
                    for dois in list:
                        doiVenues.append(dois)
                        internalId.append(venuesIdValue_s[e][0])
                        issnVenues.append(venues_id_issn[e][0])
            internalId_s=Series(internalId)
            doi_s=Series(doiVenues)
            issn_s=Series(issnVenues)
            standaloneVenues_ids_df=DataFrame({"venueinternalId_s":internalId_s, "venues doi":doi_s, "issn":issn_s})

            ref_doi=[]
            ref_cit=[]
            for key in references:
                referenceValue = references[key]
                ref_doi.append(key)
                ref_cit.append(referenceValue)
            doiReferences=Series(ref_doi)
            citReferences=Series(ref_cit)
            # === REFERENCES DATAFRAME POPULATED:
            ref_df=DataFrame({
                "ref doi" : doiReferences,
                "citation" : citReferences, 
            })

            publisherCrossref=[]
            pub_id=[]
            pub_name=[]
            for key in publishers:
                pub_val = publishers[key] #crossref
                publisherCrossref.append(key)
                pub_id.append(pub_val["id"])
                pub_name.append(pub_val["name"])
            publisherCrossref_s=Series(publisherCrossref)
            pub_id_s=Series(pub_id)
            pub_name_s=Series(pub_name)
            # === PUBLISHERS DATAFRAME POPULATED:
            publishers_df=DataFrame({
                "crossref" : publisherCrossref_s,
                "publisher id" : pub_id_s, 
                "name" : pub_name_s, 
            })

            venues_authors = merge(standaloneVenues_ids_df, authors_df, left_on="venues doi", right_on="auth doi", how="outer")
            venues_authors_ref = merge(venues_authors, ref_df, left_on="venues doi", right_on="ref doi", how="outer")

            authors_dict = {}
            publications_cache = {} #some publications that may have been instanciated more than once
            _ = [] #an empty list to check the type in the dataframe according to how I'm going to populate the Graph
            venues_dict = {}
            publicationPublisher_cache = {}

            if storedTriples == 0: #if my SPARQL store is empty [FIRST CASE SCENARIO]
                for idx, row in venues_authors_ref.iterrows():
                    publication_id = "publication-" + str(idx) #instantiate the first node, with the publication entity
                    subj = URIRef(base_url + publication_id)
                    #authors-related triples:
                    FF_graph.add((subj, doi, Literal(row["auth doi"]))) #as said in the documentation, the software has been developed in a publication/venue-centric modus operandi.
                    #That's why every time I have the chance to retrieve a doi, I immediately want to store it in a cache.
                    #Also and maybe more than ever when a doi comes from a json file, i.e. a source that usually contains "additional information" about publications.
                    publications_cache[row["auth doi"]] = subj
                    #publishers-related triples
                    if type(row["orcid"]) == type(_):
                        for i in range(len(row["orcid"])):
                            if row["orcid"][i] not in authors_dict:
                                author_subj = URIRef(base_url + "author-" + str(len(authors_dict)))
                                authors_dict[row["orcid"][i]] = author_subj
                                FF_graph.add((author_subj, doi, Literal(row["orcid"][i])))
                                FF_graph.add((author_subj, name, Literal(row["given name"][i])))
                                FF_graph.add((author_subj, surname, Literal(row["family name"][i])))
                            else:
                                author_subj = authors_dict[row["orcid"][i]]
                            FF_graph.add((subj, author, author_subj))
                    
                    #venues-related triples
                    if row["venues doi"] in publications_cache:
                        subj = publications_cache[row["venues doi"]]
                    else: 
                        new_idx = len(publications_cache)
                        subj = URIRef(base_url + "publication-" + str(new_idx))
                        publications_cache[row["venues doi"]] = subj
                    if type(row["issn"]) == type(_):
                        for num in range(len(row["issn"])):
                            if row["venues doi"] in venues_dict: 
                                subjVenue = venues_dict[row["venues doi"]]
                            else:
                                subjVenue = URIRef(base_url + "venue-" + str(len(venues_dict))) 
                                venues_dict[row["venues doi"]] = subjVenue
                            FF_graph.add((subjVenue, doi, Literal(row["issn"][num])))          
                            FF_graph.add((subj, publicationVenue, subjVenue))
                    
                    
                    #citations-related triples
                    if row["ref doi"] in publications_cache:
                        subj = publications_cache[row["ref doi"]]
                    else: 
                        alfa = len(publications_cache)
                        subj = URIRef(base_url + "publication-" + str(alfa))
                        FF_graph.add((subj, doi, Literal(row["ref doi"])))
                        publications_cache[row["ref doi"]] = subj
                    if type(row["citation"]) == type(_):
                        for c in range(len(row["citation"])):
                            if row["citation"][c] in publications_cache:
                                cited_publ = publications_cache[row["citation"][c]]
                            else:
                                other_idx = len(publications_cache)
                                cited_publ = URIRef(base_url + "publication-" + str(other_idx))
                                publications_cache[row["citation"][c]] = cited_publ
                            FF_graph.add((subj, citation, cited_publ))
                
                for idx, row in publishers_df.iterrows():
                    subj = URIRef(base_url + "publisher-" + str(idx))
                    FF_graph.add((subj, doi, Literal(row["crossref"])))
                    FF_graph.add((subj, title, Literal(row["name"])))        
                
            elif storedTriples > 0: #if my SPARQL store has been already populated [SECOND CASE SCENARIO]
                # I check what authors data have been already put in the store
                trackAuthors = """
                PREFIX schema: <https://schema.org/>

                SELECT ?author
                WHERE {
                    ?publication schema:author ?author .
                }
                """
                authors_query_df = (get(endpoint, trackAuthors, True)).shape[0]
                #I check what venues have been already put in the store
                trackVenues = """
                PREFIX schema: <https://schema.org/>

                SELECT ?venue
                WHERE {
                    ?publication schema:isPartOf ?venue .
                }
                """
                venues_query_df = (get(endpoint, trackVenues, True)).shape[0]

                pQuery = """
                PREFIX schema: <https://schema.org/>
                SELECT ?publication
                WHERE {
                    ?publication schema:identifier ?identifier .
                    FILTER regex(?identifier, "doi")
                }
                """
                result_df = get(endpoint, pQuery, True)
                numberOfPublication = result_df.shape[0]
                for idx, row in venues_authors_ref.iterrows():
                    query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    comparePublications = get(endpoint, query.format(row["auth doi"]), True)
                    if comparePublications.empty:
                        if row["auth doi"] in publications_cache:
                            subj = publications_cache[row["auth doi"]]
                        else:
                            numb = numberOfPublication + len(publications_cache)
                            subj = URIRef(base_url + "publication-" + str(numb))
                            publications_cache[row["auth doi"]] = subj
                        
                    else:
                        subj = URIRef(comparePublications.at[0, "publication"])                 
                    if type(row["orcid"]) == type(_):
                        for r in range(len(row["orcid"])):
                            aQuery = """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?author
                            WHERE {{
                                ?publication schema:author ?author .
                                ?author schema:identifier "{0}"
                            }}
                            """
                            compareAuth = get(endpoint, aQuery.format(row["orcid"][r]), True)   
                            if compareAuth.empty:                         
                                if row["orcid"][r] not in authors_dict:
                                    author_subj = URIRef(base_url + "author-" + str(len(authors_dict) + authors_query_df))
                                    authors_dict[row["orcid"][r]] = author_subj
                                    FF_graph.add((author_subj, doi, Literal(row["orcid"][r])))
                                    FF_graph.add((author_subj, name, Literal(row["given name"][r])))
                                    FF_graph.add((author_subj, surname, Literal(row["family name"][r])))
                                else: 
                                    author_subj = authors_dict[row["orcid"][r]]
                            else:
                                author_subj = URIRef(compareAuth.at[0, "author"])
                            FF_graph.add((subj, author, author_subj))
                    query_two = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    compareAtr = get(endpoint, query_two.format(row["venues doi"]), True)
                    if compareAtr.empty:
                        if row["venues doi"] in publications_cache:
                            subj = publications_cache[row["venues doi"]]
                        else:
                            number_of_index = numberOfPublication + len(publications_cache)
                            subj = URIRef(base_url + "publication-" + str(number_of_index))
                            publications_cache[row["venues doi"]] = subj
                    else:
                        subj = URIRef(compareAtr.at[0, "publication"])
                    if type(row["issn"]) == type(_):
                        for e in range(len(row["issn"])):
                            venue_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?venue
                            WHERE {{
                                <{0}> schema:isPartOf ?venue .
                            }}
                            """
                            compareVenues = get(endpoint, venue_query.format(subj), True)
                            if compareVenues.empty:
                                if row["venues doi"] in venues_dict:
                                    subjVenue = venues_dict[row["venues doi"]]
                                else:
                                    subjVenue = URIRef(base_url + "venue-" + str(venues_query_df +len(venues_dict)))
                                    venues_dict[row["venues doi"]] = subjVenue
                                    FF_graph.add((subjVenue, doi, Literal(row["issn"][e])))
                                FF_graph.add((subj, publicationVenue, subjVenue))
                            else:
                                subjVenue = URIRef(compareVenues.at[0, "venue"])
                                FF_graph.add((subjVenue, doi, Literal(row["issn"][e])))
                    
                    doi_query= """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    doi_df = get(endpoint, doi_query.format(row["ref doi"]), True)
                    if doi_df.empty:
                        if row["ref doi"] in publications_cache:
                            subj = publications_cache[row["ref doi"]] 
                        else: 
                            new_idx = len(publications_cache)
                            subj = URIRef(base_url + "publication-" + str(new_idx + numberOfPublication))
                            FF_graph.add((subj, doi, Literal(row["ref doi"])))
                            publications_cache[row["ref doi"]] = subj
                    if type(row["citation"]) == type(_):
                        for w in range(len(row["citation"])):
                            query_four= """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?publication
                            WHERE {{
                                ?publication schema:identifier "{0}" .
                            }}
                            """
                            check_doi_four = get(endpoint, query_four.format(row["citation"][w]), True)
                            if check_doi_four.empty:
                                if row["citation"][a] in publications_cache:
                                    cited_publ = publications_cache[row["citation"][a]]
                                else:
                                    number_of_index = numberOfPublication + len(publications_cache)
                                    cited_publ = URIRef(base_url + "publication-" + str(number_of_index))
                                    publications_cache[row["citation"][a]] = subj
                            else:
                                cited_publ = URIRef(check_doi_four.at[0, "publication"])
                            FF_graph.add((subj, citation, cited_publ))
                for idx, row in publishers_df.iterrows():
                    pub_query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publisher
                    WHERE {{
                        ?publisher schema:identifier "{0}" .
                    }}
                    """
                    check_pub = get(endpoint, pub_query.format(row["crossref"]), True)
                    if check_pub.empty:
                        num_publisher = """
                        PREFIX schema: <https://schema.org/>
                        SELECT ?publisher
                        WHERE {
                            ?publication schema:publisher ?publisher .
                        }
                        """
                        pub_idx = get(endpoint, num_publisher, True).shape[0]
                        subj = URIRef(base_url + "publisher-" + str(pub_idx + len(publicationPublisher_cache)))
                        FF_graph.add((subj, doi, Literal(row["crossref"])))
                        FF_graph.add((subj, title, Literal(row["name"])))
                        publicationPublisher_cache[row["crossref"]] = subj
                    else:
                        subj = URIRef(check_pub.at[0, 'publisher'])
                        FF_graph.add((subj, title, Literal(row["name"])))                    
        
        store.open((endpoint, endpoint))
        for triple in FF_graph.triples((None, None, None)):
            store.add(triple)
        store.close()
        return isinstance(path, str)
        
        

class TriplestoreQueryProcessor(TriplestoreProcessor):
    def __init__(self) -> None:
        super().__init__()
    
    def getPublicationsPublishedInYear(self, year): 
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?orcid ?surname ?publisher ?pub_venue ?venue_type ?event
        WHERE {{
            ?publication schema:name ?title ;
            dcterms:date ?publicationYear ;
            rdf:type ?type ;
            schema:isPartOf ?pub_venue ;
            schema:author ?author ;
            schema:publisher ?publisher .
            ?author schema:givenName ?name ; 
            ?author schema:familyName ?surname .  
            ?pub_venue rdf:type ?venue_type ;
            ?author schema:identifier ?orcid .
            ?publication schema:publisher ?publisher .
            FILTER (?publicationYear = {0}) .
            OPTIONAL {
              ?publication schema:issueNumber ?issue }.
          	OPTIONAL {?publication schema:volumeNumber ?volume                                   
                                   }
            OPTIONAL {?publication schema:numberedPosition ?chapter}
        }}
        """
        publ = get(endpoint, new_query.format(year), True)
        return publ
    
    def getPublicationsByAuthorId(self, orcid):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?surname ?orcid ?citation ?publisher ?pub_venue ?venue_type ?pub_name ?event
        WHERE {{
            ?publication schema:name ?title .
            ?publication schema:datePublished ?publicationYear .
            ?publication rdf:type ?type .
            ?publication schema:isPartOf ?pub_venue .
            ?pub_venue rdf:type ?venue_type .
            ?publication schema:author ?author .
            ?author schema:givenName ?name .
            ?author schema:familyName ?surname .
            ?author schema:identifier ?orcid .
            FILTER (?orcid = "{0}") .
            ?publication schema:publisher ?publisher .
            ?publisher schema:name ?pub_name
            OPTIONAL {{?publication schema:issueNumber ?issue .
            ?publication schema:volumeNumber ?volume }} 
            OPTIONAL {{?publication schema:numberedPosition ?chapter}} 
            OPTIONAL {{?publication schema:citation ?citation}} 
        }}
        """
        publ = get(endpoint, new_query.format(orcid), True)
        return publ
    
    def getPublicationsByAuthorId(self, orcid):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?surname ?orcid ?citation ?publisher ?pub_venue ?venue_type ?pub_name ?event
        WHERE {{
            ?publication schema:name ?title .
            ?publication schema:datePublished ?publicationYear .
            ?publication rdf:type ?type .
            ?publication schema:isPartOf ?pub_venue .
            ?pub_venue rdf:type ?venue_type .
            ?publication schema:author ?author .
            ?author schema:givenName ?name .
            ?author schema:familyName ?surname .
            ?author schema:identifier ?orcid .
            FILTER (?orcid = "{0}") .
            ?publication schema:publisher ?publisher .
            ?publisher schema:name ?pub_name
            OPTIONAL {{?publication schema:issueNumber ?issue .
            ?publication schema:volumeNumber ?volume }} 
            OPTIONAL {{?publication schema:numberedPosition ?chapter}} 
            OPTIONAL {{?publication schema:citation ?citation}} 
        }}
        """
        publ = get(endpoint, new_query.format(orcid), True)
        return publ
        
    def getMostCitedPublication(self): 
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?citation (COUNT(?citation) AS ?cited) 
        WHERE { 
        ?publication schema:citation ?citation .
        }
        GROUP BY ?citation
        ORDER BY desc(?cited)
        """
        publ = get(endpoint, new_query, True)
        most_cited = publ.at[0, "citation"]
        return most_cited.head

    def getMostCitedVenue(self): 
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?venue (COUNT(?venue) AS ?cited) 
        WHERE { 
        ?publication schema:citation ?citation .
        ?citation schema:isPartOf ?venue
        }
        GROUP BY ?venue
        ORDER BY desc(?cited) 
        """
        most_cited = get(endpoint, new_query, True).at[0, "venue"]
        return most_cited.head
    
    def getVenuesByPublisherId(self, pub_id):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?pub_venue ?venue_name ?venue_id ?venue_type ?publisher ?pub_id ?pub_name ?event
        WHERE {{
            ?publication schema:isPartOf ?pub_venue ;
            schema:publisher ?publisher .
            ?pub_venue schema:identifier ?venue_id ;
            schema:name ?venue_name .
            
            VALUES ?venue_type {
                  <https://schema.org/ScholarlyArticle>
                  <https://schema.org/Chapter>
                  <https://schema.org/ProceedingsPaper> 
                  ?pub_venue rdf:type ?venue_type .
            ?publication 
            ?publisher schema:identifier ?pub_id .
            FILTER (?pub_id = "{0}") .
            ?publisher schema:name ?pub_name 
        }}
        """
        publ = get(endpoint, new_query.format(pub_id), True)
        return publ
    
    def getPublicationInVenue(self, venue):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?surname ?orcid ?citation ?publisher ?pub_venue ?venue_type ?pub_name ?event
        WHERE {{
            ?publication schema:name ?title .
            ?publication schema:datePublished ?publicationYear .
            ?publication rdf:type ?type .
            ?publication schema:isPartOf ?pub_venue .
            ?pub_venue schema:identifier ?pub_id .
            ?pub_venue rdf:type ?venue_type .
            ?publication schema:author ?author .
            ?author schema:givenName ?name .
            ?author schema:familyName ?surname .
            ?author schema:identifier ?orcid .
            FILTER (?pub_id = "{0}") .
            ?publication schema:publisher ?publisher .
            ?publisher schema:name ?pub_name .
            OPTIONAL {{?publication schema:issueNumber ?issue .
            ?publication schema:volumeNumber ?volume }} 
            OPTIONAL {{?publication schema:numberedPosition ?chapter}} 
            OPTIONAL {{?publication schema:citation ?citation}}    
        }}  
        """
        publ = get(endpoint, new_query.format(venue), True)
        return publ

    def getJournalArticlesInIssue (self, issn, volume ,issue):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?surname ?orcid ?citation ?publisher ?pub_venue ?venue_type ?pub_name ?event
        WHERE {{
            ?publication schema:name ?title ;
            schema:author ?author ;
            schema:datePublished ?publicationYear ;
            schema:isPartOf ?pub_venue ;
            schema:publisher ?publisher ;
            rdf:type ?type .
            VALUES ?type {
                  <https://schema.org/ScholarlyArticle>
                  <https://schema.org/Chapter>
                  <https://schema.org/ProceedingsPaper>
                  }     
            ?pub_venue schema:identifier ?pub_id ;
            rdf:type ?venue_type .
            VALUES ?venue_type {
                <https://schema.org/Chapter>
                <https://schema.org/Periodical>
                <https://schema.org/Book>
            }
            ?author schema:givenName ?name ;
            schema:familyName ?surname ;
            schema:identifier ?orcid .
            FILTER (?pub_id = "{0}") . 
            ?publisher schema:name ?pub_name .
            ?publication schema:volumeNumber "{1}" .
            ?publication schema:issueNumber "{2}" .
            OPTIONAL {{?publication schema:citation ?citation}}    
        }}  
        """
        publ = get(endpoint, new_query.format(issn, volume, issue), True)
        return publ

    def getJournalArticlesInVolume(self,volume,issn):
        pass
    def getJournalArticlesInJournal(self,issn):
        pass
    def getProceedingsByEvent(eventname):
        pass
    def getPublicationAuthors(self,doi):
        pass
    def getPublicationsByAuthorName(self,partialname):
        pass
    def getDistinctPublishersOfPublications(self,pubidlist):
        pass

