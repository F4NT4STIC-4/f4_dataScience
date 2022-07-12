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
    # return annotation because Triplestore data processor doesn't return anything but the boolean value and the populated FF_graph through its method uploadData.
    def __init__(self) -> None:
        super().__init__()

    def uploadData(self, path):
        # input: user provided path of a csv or json file.
        # output: a boolean value.
        # Once instanciated all the triples in the FF_graph, the function uploadData also provides the updating of the SPARQL store into the BlazegraphDB.

        # === STEP 1: INSTANTIATE THE ENDPOINT AND OPEN-CLOSE THE CONNECTION TO THE STORE === #
        # Firstly, I want to get the endpoint, in order to open the connection to the BlazegraphDB and explore it.
        # Secondly, I want to keep track of the "current situation" of the database before storing in it new triples, e.g. if there are already existent triples in it or not (this is done in the case the function uploadData is called multiple times).
        # Finally, I can close the connection to store the FF_Graph triples.
        endpoint = self.getEndpointUrl()
        store = SPARQLUpdateStore()
        store.open((endpoint, endpoint))
        storedTriples = store.__len__(context=None)
        store.close()

        # === STEP 2: CREATE THE RDF GRAPH AND POPULATE IT === #

        FF_graph = Graph()

        # classes of resources
        JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
        BookChapter = URIRef("https://schema.org/Chapter")
        Journal = URIRef("https://schema.org/Periodical")
        Book = URIRef("https://schema.org/Book")
        Proceedings = URIRef("https://schema.org/EventSeries")

        # attributes related to classes
        # doi is valid for every unique identifier(change the link) so I'll use different regex while querying to make sure that I'm addressing the right unique identifier in my interest.
        doi = URIRef("https://schema.org/identifier")
        publicationYear = URIRef("https://schema.org/datePublished")
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

        base_url = "https://FF.github.io/res/"
        #<<<<<<< [CSV FILE FROM PATH] >>>>>>>>#
        if ".csv" in path:
            publicationVenue_cache = {}  # caches here are counters in the form of dictionaries
            publicationPublisher_cache = {}
            publicationVenueIdx_cache = 0
            # I need a publicationCounter to know how many publications are in my Store.
            publicationCounter = 0
            publications = read_csv(path, keep_default_na=False,
                                    dtype={
                                        "id": "string",
                                        "title": "string",
                                        "type": "string",
                                        "publication_year": "int",
                                        "issue": "string",
                                        "volume": "string",
                                        "chapter": "string",
                                        "publication_venue": "string",
                                        "venue_type": "string",
                                        "publisher": "string",
                                        "event": "string"
                                    })
            if storedTriples == 0:  # if my SPARQL store is empty
                # --- PUBLICATIONS --- #
                for idx, row in publications.iterrows():
                    # instantiate the first node to build the triples
                    publication_id = "publication-" + str(idx)
                    # publication as subject
                    subj = URIRef(base_url + publication_id)
                    # basic publication triples:
                    FF_graph.add((subj, doi, Literal(row["id"])))
                    FF_graph.add((subj, title, Literal(row["title"])))
                    FF_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    # triples for Journal Article class:
                    if row["type"] == "journal-article":
                        FF_graph.add((subj, RDF.type, JournalArticle))
                        FF_graph.add((subj, issue, Literal(row["issue"])))
                        FF_graph.add((subj, volume, Literal(row["volume"])))
                    # triples for Book Chapter class:
                    elif row["type"] == "book-chapter":
                        FF_graph.add((subj, RDF.type, BookChapter))
                        FF_graph.add((subj, chapter_num, Literal(row["chapter"])))
                    # --- VENUES --- I check whether the Venues are present or not in my cache #
                    publicationVenueValue = row["publication_venue"]
                    if publicationVenueValue not in publicationVenue_cache:
                        # since the store is empty, the first venuesubject-related tripleI'll add qill be the first in absolute.
                        venues_id = "venue-" + str(len(publicationVenue_cache))
                        subjVenue = URIRef(base_url + venues_id)
                        # the value is now part of my cache
                        publicationVenue_cache[publicationVenueValue] = subjVenue
                        # Publication-hasVenue-Venue triples:
                        FF_graph.add((subj, publicationVenue, subjVenue))
                        venueTypeValue = row["venue_type"]
                        # triples for Journal class:
                        if venueTypeValue == "journal":
                            FF_graph.add((subjVenue, RDF.type, Journal))
                        # triples for Book class:
                        elif venueTypeValue == "book":
                            FF_graph.add((subjVenue, RDF.type, Book))
                        # triple for Proceedings class:
                        elif venueTypeValue == "proceedings":
                            FF_graph.add((subjVenue, RDF.type, Proceedings))
                        # triple for Publication Venue Value:
                        FF_graph.add((subjVenue, title, Literal(publicationVenueValue)))

                    elif publicationVenueValue in publicationVenue_cache:
                        subjVenue = publicationVenue_cache[publicationVenueValue]
                        FF_graph.add((subj, publicationVenue, subjVenue))

                    publisherValue = row["publisher"]
                    if publisherValue not in publicationPublisher_cache:
                        subjPublisher = URIRef(
                            base_url + "publisher-" + str(len(publicationPublisher_cache)))
                        # the value is now part of my cache
                        publicationPublisher_cache[publisherValue] = subjPublisher
                        # Publisher related triples -> (Publication-hasPublisher-Publisher(Name)) | (Publisher-isPublisherOf, PublicationDoi) #nb: doi is for every unique identifier, so be careful a moment
                        FF_graph.add((subj, publisher, subjPublisher))
                        FF_graph.add((subjPublisher, doi, Literal(publisherValue)))
                    else:
                        subjPublisher = publicationPublisher_cache[publisherValue]
                        # publication has Publisher(Name)
                        FF_graph.add((subj, publisher, subjPublisher))

            elif storedTriples > 0:  # if my SPARQL store has been already set with some triples, I want to track what element and how many elements are there in it.
                publication_query = """
        PREFIX schema: <https://schema.org/>
        SELECT ?publication
        WHERE {
          ?publication schema:identifier ?identifier .
          FILTER regex(?identifier, "doi")
        }
        """
                publicationOutput = get(endpoint, publication_query, True)
                numberOfPublication = publicationOutput.shape[0]

                venues_query = """
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?venue
        WHERE {
        ?publ schema:isPartOf ?venue .
        }
        """
                publicationVenueIdx_cache = get(
                    endpoint, venues_query, True).shape[0]  # cambiare le variabili qui.
                for idx, row in publications.iterrows():
                    # value present in the dataframe, to search in the existing store
                    publicationDoiValues = row["id"]
                    comparePublications_query = """
            PREFIX schema: <https://schema.org/>
            SELECT ?publication
            WHERE {{
                ?publication schema:identifier "{0}" . 
            }}
            """
                    comparePublicationsOutput = get(
                        endpoint, comparePublications_query.format(publicationDoiValues), True)
                    # --- PUBLICATIONS --- #
                    # if there are no doi stored in the output dataframe from the previous query, i.e. if that doi value is not in anytriple as subject:
                    if comparePublicationsOutput.empty:
                        subj = URIRef(base_url + "publication-" +
                                      str(numberOfPublication + publicationCounter))
                        publicationCounter += 1
                        FF_graph.add(
                            (subj, doi, Literal(publicationDoiValues)))
                    # --- VENUES --- After querying about dois, now in the same condition loop I insert another query, to complete the framework "publication-has-publication-venue"; I expect that there will be no venue value stored yet, unless it comes from another csv file uploading operation.
                        venue_query = """
              PREFIX schema: <https://schema.org/>
              SELECT ?venue
              WHERE {{
                  ?publication schema:isPartOf ?venue .
                  ?venue schema:name "{0}" .
              }}
              """
                        comparePublicationVenuesOutput = get(
                            endpoint, venue_query.format(row["publication_venue"]), True)
                        if comparePublicationVenuesOutput.empty:
                            if row["publication_venue"] in publicationVenue_cache:
                                subjVenue = publicationVenue_cache[row["publication_venue"]]
                            else:
                                subjVenue = URIRef(
                                    base_url + "venue-" + str(publicationVenueIdx_cache + len(publicationVenue_cache)))
                                publicationVenue_cache[row["publication_venue"]
                                                       ] = subjVenue
                                # Journal Class-related triple
                                if row["venue_type"] == "journal":
                                    FF_graph.add(
                                        (subjVenue, RDF.type, Journal))
                                # Book Class-related triples
                                elif row["venue_type"] == "book":
                                    FF_graph.add((subjVenue, RDF.type, Book))
                                   # Proceedings Class-related triples
                                elif row["venue_type"] == "proceedings":
                                    FF_graph.add(
                                        (subjVenue, RDF.type, Proceedings))
                            FF_graph.add(
                                (subjVenue, title, Literal(row["publication_venue"])))
                        else:  # if the doi value is already stored in the graph:
                            subjVenue = URIRef(
                                comparePublicationVenuesOutput.at[0, "venue"])
                            if row["venue_type"] == "journal":
                                FF_graph.add((subjVenue, RDF.type, Journal))
                            elif row["venue_type"] == "book":
                                FF_graph.add((subjVenue, RDF.type, Book))
                            elif row["venue_type"] == "proceedings":
                                FF_graph.add(
                                    (subjVenue, RDF.type, Proceedings))
                            FF_graph.add(
                                (subjVenue, title, Literal(row["publication_venue"])))
                        FF_graph.add((subj, publicationVenue, subjVenue))
                    else:  # if I have already some dois in the graph registered as subject
                        publ_query = """
                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>

                SELECT ?publication
                WHERE {{
                    ?publication schema:identifier "{0}"
                }}
                """
                        subj = URIRef(get(endpoint, publ_query.format(
                            publicationDoiValues), True).at[0, 'publication'])
                        # I check if a doi is already linked to a venue
                        venue_query = """
                PREFIX schema: <https://schema.org/>
                SELECT ?venue
                WHERE {{
                    ?publication schema:isPartOf ?venue .
                    ?publication schema:identifier "{0}" .
                }}
                """
                        comparePublicationAndVenues = get(
                            endpoint, venue_query.format(publicationDoiValues), True)
                        if comparePublicationAndVenues.empty:
                            if row["publication_venue"] in publicationVenue_cache:
                                subjVenue = publicationVenue_cache[row["publication_venue"]]
                            else:
                                venue_query = """
                        PREFIX schema: <https://schema.org/>
                        SELECT ?venue
                        WHERE {{
                            ?publication schema:isPartOf ?venue .
                            ?venue schema:name "{0}" .
                        }}
                        """
                                compareVenues = get(endpoint, venue_query.format(
                                    row["publication_venue"]), True)
                                if compareVenues.empty:
                                    subjVenue = URIRef(
                                        base_url + "venue-" + str(publicationVenueIdx_cache + len(publicationVenue_cache)))
                                    publicationVenue_cache[row["publication_venue"]
                                                           ] = subjVenue
                                    if row["venue_type"] == "journal":
                                        FF_graph.add(
                                            (subjVenue, RDF.type, Journal))
                                    elif row["venue_type"] == "book":
                                        FF_graph.add(
                                            (subjVenue, RDF.type, Book))
                                    FF_graph.add(
                                        (subjVenue, title, Literal(row["publication_venue"])))
                                else:
                                    subjVenue = URIRef(
                                        compareVenues.at[0, 'venue'])
                        else:
                            subjVenue = URIRef(
                                comparePublicationAndVenues.at[0, 'venue'])

                        if row["venue_type"] == "journal":
                            FF_graph.add((subjVenue, RDF.type, Journal))
                        elif row["venue_type"] == "book":
                            FF_graph.add((subjVenue, RDF.type, Book))
                        FF_graph.add(
                            (subjVenue, title, Literal(row["publication_venue"])))
                        FF_graph.add((subj, publicationVenue, subjVenue))

                    if row["type"] == "journal-article":
                        FF_graph.add((subj, RDF.type, JournalArticle))
                        FF_graph.add((subj, issue, Literal(row["issue"])))
                        FF_graph.add((subj, volume, Literal(row["volume"])))
                    elif row["type"] == "book-chapter":
                        FF_graph.add((subj, RDF.type, BookChapter))
                        FF_graph.add(
                            (subj, chapter_num, Literal(row["chapter"])))
                    FF_graph.add((subj, title, Literal(row["title"])))
                    FF_graph.add(
                        (subj, publicationYear, Literal(row["publication_year"])))

                    comparePublishers_query = """
            PREFIX schema:<https://schema.org/>
            SELECT ?publisher
            WHERE {{
                ?publisher schema:identifier "{0}"
            }}
            """
                    comparePublishersOutput = get(
                        endpoint, comparePublishers_query.format(row["publisher"]), True)
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
                            publiOutput = get(
                                endpoint, publi_query, True).shape[0]
                            subjPublisher = URIRef(
                                base_url + "publisher-" + str(len(publicationPublisher_cache) + publiOutput))
                            FF_graph.add((subj, publisher, subjPublisher))
                            FF_graph.add(
                                (subjPublisher, doi, Literal(row["publisher"])))
                            publicationPublisher_cache[row["publisher"]
                                                       ] = subjPublisher
                    else:
                        subjPublisher = URIRef(
                            comparePublishersOutput.at[0, "publisher"])
                        FF_graph.add((subj, publisher, subjPublisher))

        elif ".json" in path:
            with open(path, "r", encoding="utf-8") as file:
                # since json is an unstructured datatype, I can see it as a dictionary of dictionaries.
                jsondata = load(file)

            authors = jsondata.get("authors")
            venues = jsondata.get("venues_id")
            references = jsondata.get("references")
            publishers = jsondata.get("publishers")

            # lists to store into Series to store into a new Dataframe
            doi_authors = []
            family = []
            given = []
            orcid = []

            for key in authors:
                a = []  # a list for family value
                b = []  # b list for given value
                c = []  # c list for orcid value
                authorValue = authors[key]
                doi_authors.append(key)
                for dict in authorValue:  # values of the author
                    a.append(dict["family"])
                    b.append(dict["given"])
                    c.append(dict["orcid"])
                family.append(a)
                given.append(b)
                orcid.append(c)

                fromDoiAuthors_s = Series(doi_authors)
            fromFamily_s = Series(family)
            fromGiven_s = Series(given)
            fromOrcid_s = Series(orcid)

            authors_df = DataFrame({
                "auth doi": fromDoiAuthors_s,
                "family name": fromFamily_s,
                "given name": fromGiven_s,
                "orcid": fromOrcid_s
            })

            # venues
            doi_venues_id = []
            issn_isbn = []

            for key in venues:  # I iterate over the venues json object, assigning to the key the value of the venue and to the arrays values the data to describe the venue, i.e. the unique identifier issn or isbn if it's a venue of type book.
                venuesValue = venues[key]
                doi_venues_id.append(key)
                issn_isbn.append(venuesValue)
            standaloneVenues = []  # cache of issn
            doi_Cache = []

            # here I iterate over the range of the lenght of the list, to obtain a unique index.
            for i in range(len(issn_isbn)):
                if issn_isbn[i] not in standaloneVenues:
                    standaloneVenues.append(issn_isbn[i])
                    dois = []
                    dois.append(doi_venues_id[i])
                    doi_Cache.append(dois)
                else:
                    j = standaloneVenues.index(issn_isbn[i])
                    doi_Cache[j].append(doi_venues_id[i])

            standaloneDoiVenues_s = Series(doi_Cache)  # standalone series
            standaloneIssn_s = Series(standaloneVenues)
            standaloneVenues_df = DataFrame({
                "venues doi": standaloneDoiVenues_s,
                "issn": standaloneIssn_s,
            })

            venuesValue_df = standaloneVenues_df[["venues doi", "issn"]]
            venues_Id = []
            for idx, row in venuesValue_df.iterrows():
                venues_Id.append("venues-" + str(idx))
            # I choose to insert another space, for internal id purposes
            venuesValue_df.insert(
                0, "venues id", Series(venues_Id, dtype="string"))
            venuesIdValue_s = venuesValue_df.filter(
                ["venues id"])  # I extract the data
            venuesIdValue_s = venuesIdValue_s.values.tolist()  # Transform it into a list
            doi_venues_id = venuesValue_df.filter(["venues doi"])
            doi_venues_id = doi_venues_id.values.tolist()
            venues_id_issn = venuesValue_df.filter(["issn"])
            venues_id_issn = venues_id_issn.values.tolist()

            internalId = []
            doiVenues = []
            issnVenues = []
            for out in doi_venues_id:  # iterating over every series
                i = doi_venues_id.index(out)
                for list in out:
                    for dois in list:
                        doiVenues.append(dois)
                        internalId.append(venuesIdValue_s[i][0])
                        issnVenues.append(venues_id_issn[i][0])
            internalId_s = Series(internalId)
            doi_s = Series(doiVenues)
            issn_s = Series(issnVenues)
            standaloneVenues_ids_df = DataFrame(
                {"venueinternalId_s": internalId_s, "venues doi": doi_s, "issn": issn_s})

            # references
            ref_doi = []
            ref_cit = []

            for key in references:
                referenceValue = references[key]
                ref_doi.append(key)
                ref_cit.append(referenceValue)

            doiReferences = Series(ref_doi)
            citReferences = Series(ref_cit)

            ref_df = DataFrame({
                "ref doi": doiReferences,
                "citation": citReferences,
            })

            # publishers
            publisherCrossref = []
            pub_id = []
            pub_name = []

            for key in publishers:
                pub_val = publishers[key]  # crossref
                publisherCrossref.append(key)
                pub_id.append(pub_val["id"])
                pub_name.append(pub_val["name"])

            publisherCrossref_s = Series(publisherCrossref)
            pub_id_s = Series(pub_id)
            pub_name_s = Series(pub_name)

            publishers_df = DataFrame({
                "crossref": publisherCrossref_s,
                "publisher id": pub_id_s,
                "name": pub_name_s,
            })

            venues_authors = merge(standaloneVenues_ids_df, authors_df,
                                   left_on="venues doi", right_on="auth doi", how="outer")
            venues_authors_ref = merge(
                venues_authors, ref_df, left_on="venues doi", right_on="ref doi", how="outer")

            authors_dict = {}
            publications_cache = {}  # some publications may have been put again
            _ = []  # an empty list to check the type in the dataframe according to how I'm going to populate the Graph
            venues_dict = {}
            publicationPublisher_cache = {}

            if storedTriples == 0:
                for idx, row in venues_authors_ref.iterrows():
                    publication_id = "publication-" + str(idx)
                    subj = URIRef(base_url + publication_id)
                    # authors
                    FF_graph.add((subj, doi, Literal(row["auth doi"])))
                    publications_cache[row["auth doi"]] = subj
                    if type(row["orcid"]) == type(_):
                        for i in range(len(row["orcid"])):
                            if row["orcid"][i] not in authors_dict:
                                author_subj = URIRef(
                                    base_url + "author-" + str(len(authors_dict)))
                                authors_dict[row["orcid"][i]] = author_subj
                                FF_graph.add(
                                    (author_subj, doi, Literal(row["orcid"][i])))
                                FF_graph.add(
                                    (author_subj, name, Literal(row["given name"][i])))
                                FF_graph.add(
                                    (author_subj, surname, Literal(row["family name"][i])))
                            else:
                                author_subj = authors_dict[row["orcid"][i]]
                            FF_graph.add((subj, author, author_subj))

                    # venues
                    if row["venues doi"] in publications_cache:
                        subj = publications_cache[row["venues doi"]]
                    else:
                        new_idx = len(publications_cache)
                        subj = URIRef(base_url + "publication-" + str(new_idx))
                        publications_cache[row["venues doi"]] = subj
                    if type(row["issn"]) == type(_):
                        for j in range(len(row["issn"])):
                            if row["venues doi"] in venues_dict:
                                subjVenue = venues_dict[row["venues doi"]]
                            else:
                                subjVenue = URIRef(
                                    base_url + "venue-" + str(len(venues_dict)))
                                venues_dict[row["venues doi"]] = subjVenue
                            FF_graph.add(
                                (subjVenue, doi, Literal(row["issn"][j])))
                            FF_graph.add((subj, publicationVenue, subjVenue))

                    # citations
                    if row["ref doi"] in publications_cache:
                        subj = publications_cache[row["ref doi"]]
                    else:
                        new_idx = len(publications_cache)
                        subj = URIRef(base_url + "publication-" + str(new_idx))
                        FF_graph.add((subj, doi, Literal(row["ref doi"])))
                        publications_cache[row["ref doi"]] = subj
                    if type(row["citation"]) == type(_):
                        for w in range(len(row["citation"])):
                            if row["citation"][w] in publications_cache:
                                cited_publ = publications_cache[row["citation"][w]]
                            else:
                                other_idx = len(publications_cache)
                                cited_publ = URIRef(
                                    base_url + "publication-" + str(other_idx))
                                publications_cache[row["citation"]
                                                   [w]] = cited_publ
                            FF_graph.add((subj, citation, cited_publ))

                for idx, row in publishers_df.iterrows():
                    subj = URIRef(base_url + "publisher-" + str(idx))
                    FF_graph.add((subj, doi, Literal(row["crossref"])))
                    FF_graph.add((subj, title, Literal(row["name"])))

            elif storedTriples > 0:
                # I check what authors data have been already put in the store
                authors_query = """
            PREFIX schema: <https://schema.org/>

            SELECT ?author
            WHERE {
                ?publication schema:author ?author .
            }
            """
                authors_query_df = (
                    get(endpoint, authors_query, True)).shape[0]
                # I check what venues have been already put in the store
                venues_query = """
            PREFIX schema: <https://schema.org/>

            SELECT ?venue
            WHERE {
                ?publication schema:isPartOf ?venue .
            }
            """
                venues_query_df = (get(endpoint, venues_query, True)).shape[0]

                public_query = """
            PREFIX schema: <https://schema.org/>
            SELECT ?publication
            WHERE {
                ?publication schema:identifier ?identifier .
                FILTER regex(?identifier, "doi")
            }
            """
                result_df = get(endpoint, public_query, True)
                numberOfPublication = result_df.shape[0]
                for idx, row in venues_authors_ref.iterrows():
                    query = """
                PREFIX schema: <https://schema.org/>
                SELECT ?publication
                WHERE {{
                    ?publication schema:identifier "{0}" .
                }}
                """
                    check_doi = get(endpoint, query.format(
                        row["auth doi"]), True)
                    if check_doi.empty:
                        if row["auth doi"] in publications_cache:
                            subj = publications_cache[row["auth doi"]]
                        else:
                            number_of_index = numberOfPublication + \
                                len(publications_cache)
                            subj = URIRef(
                                base_url + "publication-" + str(number_of_index))
                            publications_cache[row["auth doi"]] = subj

                    else:
                        subj = URIRef(check_doi.at[0, "publication"])
                    if type(row["orcid"]) == type(_):
                        for i in range(len(row["orcid"])):
                            auth_query = """
                        PREFIX schema: <https://schema.org/>
                        SELECT ?author
                        WHERE {{
                            ?publication schema:author ?author .
                            ?author schema:identifier "{0}"
                        }}
                        """
                            check_auth = get(
                                endpoint, auth_query.format(row["orcid"][i]), True)
                            if check_auth.empty:
                                if row["orcid"][i] not in authors_dict:
                                    author_subj = URIRef(
                                        base_url + "author-" + str(len(authors_dict) + authors_query_df))
                                    authors_dict[row["orcid"][i]] = author_subj
                                    FF_graph.add(
                                        (author_subj, doi, Literal(row["orcid"][i])))
                                    FF_graph.add(
                                        (author_subj, name, Literal(row["given name"][i])))
                                    FF_graph.add(
                                        (author_subj, surname, Literal(row["family name"][i])))
                                else:
                                    author_subj = authors_dict[row["orcid"][i]]
                            else:
                                author_subj = URIRef(
                                    check_auth.at[0, "author"])
                            FF_graph.add((subj, author, author_subj))
                    query_two = """
                PREFIX schema: <https://schema.org/>
                SELECT ?publication
                WHERE {{
                    ?publication schema:identifier "{0}" .
                }}
                """
                    check_doi_two = get(
                        endpoint, query_two.format(row["venues doi"]), True)
                    if check_doi_two.empty:
                        if row["venues doi"] in publications_cache:
                            subj = publications_cache[row["venues doi"]]
                        else:
                            number_of_index = numberOfPublication + \
                                len(publications_cache)
                            subj = URIRef(
                                base_url + "publication-" + str(number_of_index))
                            publications_cache[row["venues doi"]] = subj
                    else:
                        subj = URIRef(check_doi_two.at[0, "publication"])
                    if type(row["issn"]) == type(_):
                        for i in range(len(row["issn"])):
                            venue_query = """
                        PREFIX schema: <https://schema.org/>
                        SELECT ?venue
                        WHERE {{
                            <{0}> schema:isPartOf ?venue .
                        }}
                        """
                            compareVenues = get(
                                endpoint, venue_query.format(subj), True)
                            if compareVenues.empty:
                                if row["venues doi"] in venues_dict:
                                    subjVenue = venues_dict[row["venues doi"]]
                                else:
                                    subjVenue = URIRef(
                                        base_url + "venue-" + str(venues_query_df + len(venues_dict)))
                                    venues_dict[row["venues doi"]] = subjVenue
                                    FF_graph.add(
                                        (subjVenue, doi, Literal(row["issn"][i])))
                                FF_graph.add(
                                    (subj, publicationVenue, subjVenue))
                            else:
                                subjVenue = URIRef(
                                    compareVenues.at[0, "venue"])
                                FF_graph.add(
                                    (subjVenue, doi, Literal(row["issn"][i])))

                    # citations:
                    doi_query = """
                PREFIX schema: <https://schema.org/>
                SELECT ?publication
                WHERE {{
                    ?publication schema:identifier "{0}" .
                }}
                """
                    doi_df = get(endpoint, doi_query.format(
                        row["ref doi"]), True)
                    if doi_df.empty:
                        if row["ref doi"] in publications_cache:
                            subj = publications_cache[row["ref doi"]]
                        else:
                            new_idx = len(publications_cache)
                            subj = URIRef(base_url + "publication-" +
                                          str(new_idx + numberOfPublication))
                            FF_graph.add((subj, doi, Literal(row["ref doi"])))
                            publications_cache[row["ref doi"]] = subj
                    if type(row["citation"]) == type(_):
                        for w in range(len(row["citation"])):
                            query_four = """
                        PREFIX schema: <https://schema.org/>
                        SELECT ?publication
                        WHERE {{
                            ?publication schema:identifier "{0}" .
                        }}
                        """
                            check_doi_four = get(
                                endpoint, query_four.format(row["citation"][w]), True)
                            if check_doi_four.empty:
                                if row["citation"][w] in publications_cache:
                                    cited_publ = publications_cache[row["citation"][w]]
                                else:
                                    number_of_index = numberOfPublication + \
                                        len(publications_cache)
                                    cited_publ = URIRef(
                                        base_url + "publication-" + str(number_of_index))
                                    publications_cache[row["citation"]
                                                       [w]] = subj
                            else:
                                cited_publ = URIRef(
                                    check_doi_four.at[0, "publication"])
                            FF_graph.add((subj, citation, cited_publ))
                for idx, row in publishers_df.iterrows():
                    pub_query = """
                PREFIX schema: <https://schema.org/>
                SELECT ?publisher
                WHERE {{
                    ?publisher schema:identifier "{0}" .
                }}
                """
                    check_pub = get(endpoint, pub_query.format(
                        row["crossref"]), True)
                    if check_pub.empty:
                        num_publisher = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publisher
                    WHERE {
                        ?publication schema:publisher ?publisher .
                    }
                    """
                        pub_idx = get(endpoint, num_publisher, True).shape[0]
                        subj = URIRef(
                            base_url + "publisher-" + str(pub_idx + len(publicationPublisher_cache)))
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
            ?publication schema:name ?title .
            ?publication schema:datePublished ?publicationYear .
            ?publication rdf:type ?type .
            ?publication schema:isPartOf ?pub_venue .
            ?pub_venue rdf:type ?venue_type .
            ?publication schema:author ?author .
            ?author schema:givenName ?name .
            ?author schema:familyName ?surname .
            ?author schema:identifier ?orcid .
            ?publication schema:publisher ?publisher .
            FILTER (?publicationYear = {0}) .
            OPTIONAL {{?publication schema:issueNumber ?issue .
            ?publication schema:volumeNumber ?volume }} 
            OPTIONAL {{?publication schema:numberedPosition ?chapter}} 
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
        second_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?publication ?title ?type ?publicationYear ?issue ?volume ?chapter ?author ?name ?surname ?orcid ?citation ?publisher ?pub_venue ?venue_type ?pub_name ?event
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
            ?publication schema:name ?title .
            FILTER (?publication = <{0}>) .
            ?publication schema:publisher ?publisher .
            ?publisher schema:name ?pub_name
            OPTIONAL {{?publication schema:issueNumber ?issue .
            ?publication schema:volumeNumber ?volume }} 
            OPTIONAL {{?publication schema:numberedPosition ?chapter}} 
            OPTIONAL {{?publication schema:citation ?citation}}     
        }}
        """
        publ_df = get(endpoint, second_query.format(most_cited), True)
        return publ_df

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

        second_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?pub_venue ?venue_name ?venue_id ?venue_type ?publisher ?pub_id ?pub_name ?event
        WHERE {{
            ?publication schema:isPartOf ?pub_venue .
            ?pub_venue schema:identifier ?venue_id .
            ?pub_venue rdf:type ?venue_type .
            ?pub_venue schema:name ?venue_name .
            ?publication schema:publisher ?publisher .
            ?publisher schema:identifier ?pub_id .
            FILTER (?pub_venue = <{0}>) .
            ?publisher schema:name ?pub_name 
        }}
        """
        publ_df = get(endpoint, second_query.format(most_cited), True)
        return publ_df

    def getVenuesByPublisherId(self, pub_id):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?pub_venue ?venue_name ?venue_id ?venue_type ?publisher ?pub_id ?pub_name ?event
        WHERE {{
            ?publication schema:isPartOf ?pub_venue .
            ?pub_venue schema:identifier ?venue_id .
            ?pub_venue rdf:type ?venue_type .
            ?pub_venue schema:name ?venue_name .
            ?publication schema:publisher ?publisher .
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

    def getJournalArticlesInIssue(self, issn, volume, issue):
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
            ?publication schema:volumeNumber "{1}" .
            ?publication schema:issueNumber "{2}" .
            OPTIONAL {{?publication schema:citation ?citation}}    
        }}  
        """
        publ = get(endpoint, new_query.format(issn, volume, issue), True)
        return publ

    # def getJournalArticlesInVolume(self,volume,journalId)

    # def getJournalArticlesInJournal(self,journalId)

    # def getProceedingsByEvent(self,eventPartialName)

    # def getPublicationAuthors(self,publicationId)

    # def getPublicationsByAuthorName(self,authorPartialName)

    # def getDistinctPublishersOfPublications(self,pubIdList)


x = TriplestoreProcessor()
print("TriplestoreProcessor object\n", x)
print("Here is the EndpointUrl\n", x.getEndpointUrl())
x.setEndpointUrl("http://127.0.0.1:9999/blazegraph/sparql")
print("Here is the updated EndpointUrl\n", x.getEndpointUrl())

y = TriplestoreDataProcessor()
print("TriplestoreDataProcessor object\n", y)
y.setEndpointUrl("http://127.0.0.1:9999/blazegraph/sparql")
y.uploadData("testData/graph_publications.csv")
y.uploadData("testData/graph_other_data.json")
print("TriplestoreDataProcessor AFTER UPLOAD\n", y)

# ======== WATCH OUT ========  -------------> This was true for the Relational part but I don't know if it is true also for the Triplestore part
# If you run the code again on here, it will upload again the .csv and the .json to the database (and therefore the getMostCited method will duplicate the result!)

z = TriplestoreQueryProcessor()
print("TriplestoreQueryProcessor object\n", z)
z.setEndpointUrl("http://127.0.0.1:9999/blazegraph/sparql")

# ===== TESTS FOR ALL THE QUERIES
q1 = z.getPublicationsPublishedInYear(2020)
print("getPublicationsPublishedInYear Query\n", q1)

q2 = z.getPublicationsByAuthorId("0000-0001-9857-1511")
print("getPublicationsByAuthorId Query\n", q2)

q3 = z.getMostCitedPublication()
print("getMostCitedPublication Query\n", q3)

q4 = z.getMostCitedVenue()
print("getMostCitedVenue Query\n", q4)

q5 = z.getVenuesByPublisherId("crossref:78")
print("getVenuesByPublisherId Query\n", q5)

q6 = z.getPublicationInVenue("issn:0944-1344")
print("getPublicationInVenue Query\n", q6)

q7 = z.getJournalArticlesInIssue("9", "17", "issn:2164-5515")
print("getJournalArticleInIssue Query\n", q7)

'''
q8 = z.getJournalArticlesInVolume("17","issn:2164-5515")
print("getJournalArticleInVolume Query\n",q8)

q9 = z.getJournalArticlesInJournal("issn:2164-5515")
print("getJournalArticleInJournal Query\n",q9)

q10 = z.getProceedingsByEvent("web")
print("getProceedingsByEvent Query\n",q10)

q11 = z.getPublicationAuthors("doi:10.1080/21645515.2021.1910000")
print("getPublicationAuthors Query\n",q11)

q12 = z.getPublicationsByAuthorName("sil")
print("getPublicationsByAuthorName Query\n",q12)

q13 = z.getDistinctPublishersOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"])
print("getDistinctPublisherOfPublications Query\n",q13)
'''
