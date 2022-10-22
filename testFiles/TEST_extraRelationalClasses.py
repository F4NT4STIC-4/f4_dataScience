# ====== THIS FILE CONTAINS COMMENTS AND EXPLANATIONS ON THE QUERIES ======
from logging import raiseExceptions
from re import X
import sqlite3 as sql3
from unittest import result
from wsgiref.validate import IteratorWrapper
import pandas as pd
import json

from TEST_dataModelClasses import QueryProcessor  # WATCH OUT FOR THIS 

class RelationalProcessor(object):
    def __init__(self):
        self.dbPath = ""
    
    def getDbPath(self):
        return self.dbPath
    
    def setDbPath(self,path):
        if type(path)==str:
            self.dbPath = path
            return True
        else:
            return False


class RelationalDataProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()
    
    def uploadData(self,filepath):
        if type(filepath) != str:
            return False
        else:
            # =============== CSV UPLOAD DATA ===============
            if filepath.endswith(".csv"):
                df_publications = pd.read_csv(filepath,na_filter=False)

                # =============== PUBLICATION DATAFRAMES ===============

                journal_article_df = pd.DataFrame({
                    "issue": pd.Series(dtype="str"),
                    "volume": pd.Series(dtype="str"),
                    "publication_year": pd.Series(dtype="int"),
                    "title": pd.Series(dtype="str"),
                    "publication_venue": pd.Series(dtype="str"),
                    "id": pd.Series(dtype="str"),
                    "pub_type": pd.Series(dtype="str")
                })

                book_chapter_df = pd.DataFrame({
                    "chapter_number": pd.Series(dtype="str"),
                    "publication_year": pd.Series(dtype="int"),
                    "title": pd.Series(dtype="str"),
                    "publication_venue": pd.Series(dtype="str"),
                    "id": pd.Series(dtype="str"),
                    "pub_type": pd.Series(dtype="str")
                })

                proceeding_paper_df = pd.DataFrame({
                    "publication_year": pd.Series(dtype="int"),
                    "title": pd.Series(dtype="str"),
                    "publication_venue": pd.Series(dtype="str"),
                    "id": pd.Series(dtype="str"),
                    "pub_type": pd.Series(dtype="str")
                })

                journal_article_df['issue'] = df_publications[df_publications['type']== "journal-article"]['issue'].astype('str')
                journal_article_df['volume'] = df_publications[df_publications['type']== "journal-article"]['volume'].astype('str')
                journal_article_df['publication_year'] = df_publications[df_publications['type']== "journal-article"]['publication_year'].astype('int')
                journal_article_df['title'] = df_publications[df_publications['type']== "journal-article"]['title'].astype('str')
                journal_article_df['publication_venue'] = df_publications[df_publications['type']== "journal-article"]['publication_venue'].astype('str')
                journal_article_df['id'] = df_publications[df_publications['type']== "journal-article"]['id'].astype('str')
                journal_article_df['pub_type'] = df_publications[df_publications['type']== "journal-article"]['type'].astype('str')
                journal_article_df.replace(to_replace="nan",value="")

                book_chapter_df['publication_year'] = df_publications[df_publications['type']== "book-chapter"]['publication_year'].astype('int')
                book_chapter_df['title'] = df_publications[df_publications['type']== "book-chapter"]['title'].astype('str')
                book_chapter_df['chapter_number'] = df_publications[df_publications['type']== "book-chapter"]['chapter'].astype('str')
                book_chapter_df['publication_venue'] = df_publications[df_publications['type']== "book-chapter"]['publication_venue'].astype('str')
                book_chapter_df['id'] = df_publications[df_publications['type']== "book-chapter"]['id'].astype('str')
                book_chapter_df['pub_type'] = df_publications[df_publications['type']== "book-chapter"]['type'].astype('str')
                book_chapter_df.replace(to_replace="nan",value="")
                

                proceeding_paper_df['publication_year'] = df_publications[df_publications['type']== "proceedings-paper"]['publication_year'].astype('int')
                proceeding_paper_df['title'] = df_publications[df_publications['type']== "proceedings-paper"]['title'].astype('str')
                proceeding_paper_df['publication_venue'] = df_publications[df_publications['type']== "proceedings-paper"]['publication_venue'].astype('str')
                proceeding_paper_df['id'] = df_publications[df_publications['type'] == "proceedings-paper"]['id'].astype('str')
                proceeding_paper_df['pub_type'] = df_publications[df_publications['type']== "proceedings-paper"]['type'].astype('str')
                proceeding_paper_df.replace(to_replace="nan",value="")

                # =============== VENUES DATAFRAMES ===============

                journal_df = pd.DataFrame({
                    "name_venue": pd.Series(dtype="str"),
                    "publisher": pd.Series(dtype="str"),
                    "id_venue": pd.Series(dtype="str"),
                    "venue_type": pd.Series(dtype="str")
                })

                book_df = pd.DataFrame({
                    "name_venue": pd.Series(dtype="str"),
                    "publisher": pd.Series(dtype="str"),
                    "id_venue": pd.Series(dtype="str"),
                    "venue_type": pd.Series(dtype="str")
                })

                proceedings_df = pd.DataFrame({"event": pd.Series(dtype="str"),
                                               "name_venue": pd.Series(dtype="str"),
                                               "publisher": pd.Series(dtype="str"),
                                               "id_venue": pd.Series(dtype="str"),
                                                "venue_type": pd.Series(dtype="str")

                })

                journal_df['name_venue'] = df_publications[df_publications['venue_type'] == "journal"]['publication_venue'].astype('str')
                journal_df['publisher'] = df_publications[df_publications['venue_type'] == "journal"]['publisher'].astype('str')
                journal_df['id_venue'] = df_publications[df_publications['venue_type'] == "journal"]['id'].astype('str')
                journal_df['venue_type'] = df_publications[df_publications['venue_type'] == "journal"]['venue_type'].astype('str')
                journal_df.replace(to_replace="nan",value="")

                book_df['name_venue'] = df_publications[df_publications['venue_type'] == "book"]['publication_venue'].astype('str')
                book_df['publisher'] = df_publications[df_publications['venue_type'] == "book"]['publisher'].astype('str')
                book_df['id_venue'] = df_publications[df_publications['venue_type'] == "book"]['id'].astype('str')
                book_df['venue_type'] = df_publications[df_publications['venue_type'] == "book"]['venue_type'].astype('str')
                book_df.replace(to_replace="nan",value="")

                proceedings_df['event'] = df_publications[df_publications['venue_type'] == "proceedings"]['event'].astype('str')
                proceedings_df['name_venue'] = df_publications[df_publications['venue_type'] == "proceedings"]['publication_venue'].astype('str')
                proceedings_df['publisher'] = df_publications[df_publications['venue_type'] == "proceedings"]['publisher'].astype('str')
                proceedings_df['id_venue'] = df_publications[df_publications['venue_type'] == "proceedings"]['id'].astype('str')
                proceedings_df['venue_type'] = df_publications[df_publications['venue_type'] == "proceedings"]['venue_type'].astype('str')
                proceedings_df.replace(to_replace="nan",value="")

                # =============== DATABASE CONNECTION ===============

                with sql3.connect(self.dbPath) as rdb:
                    journal_article_df.to_sql("JournalArticleTable", rdb, if_exists="append", index=False)
                    book_chapter_df.to_sql("BookChapterTable", rdb, if_exists="append", index=False)
                    proceeding_paper_df.to_sql("ProceedingsPaperTable", rdb, if_exists="append", index=False)
                    journal_df.to_sql("JournalTable", rdb, if_exists="append", index=False)
                    book_df.to_sql("BookTable", rdb, if_exists="append", index=False)
                    proceedings_df.to_sql("ProceedingsTable", rdb, if_exists="append", index=False)
                    rdb.commit()
            
            # =============== JSON UPLOAD DATA ===============

            elif filepath.endswith(".json"):
                with open(filepath, "r", encoding="utf-8") as file:
                    jsondata = json.load(file)
                    
                    # =============== AUTHORS DATAFRAME ===============
                    authors_df = pd.DataFrame({
                        "doi_authors": pd.Series(dtype="str"),
                        "family": pd.Series(dtype="str"),
                        "given": pd.Series(dtype="str"),
                        "orcid": pd.Series(dtype="str")
                    })

                    family = []
                    given = []
                    orcid = []
                    doi_authors = []

                    authors = jsondata['authors']
                    for key in authors:
                        for value in authors[key]:
                            doi_authors.append(key)
                            family.append(value['family'])
                            given.append(value['given'])
                            orcid.append(value['orcid'])

                    authors_df['doi_authors'] = doi_authors
                    authors_df['family'] = family
                    authors_df['given'] = given
                    authors_df['orcid'] = orcid

                    # =============== VENUES DATAFRAME ===============

                    venues_id_df = pd.DataFrame({
                        "doi_venues_id": pd.Series(dtype="str"),
                        "issn_isbn": pd.Series(dtype="str"),
                    })

                    doi_venues_id = []
                    issn_isbn = []

                    venues_id = jsondata["venues_id"]
                    for key in venues_id:
                        for value in venues_id[key]:
                            doi_venues_id.append(key)
                            issn_isbn.append(value)

                    venues_id_df["doi_venues_id"] = doi_venues_id
                    venues_id_df["issn_isbn"] = pd.Series(issn_isbn)

                    # =============== REFERENCES DATAFRAME ===============

                    references_df = pd.DataFrame({
                        "og_doi": pd.Series(dtype="str"),
                        "ref_doi": pd.Series(dtype="str"),
                    })

                    og_doi = []
                    ref_doi = []

                    references = jsondata["references"]
                    for key in references:
                        for value in references[key]:
                            og_doi.append(key)
                            ref_doi.append(value)

                    references_df["og_doi"] = pd.Series(og_doi)
                    references_df["ref_doi"] = pd.Series(ref_doi)

                    # =============== PUBLISHER DATAFRAME ===============

                    publishers_df = pd.DataFrame({
                        "crossref": pd.Series(dtype="str"),
                        "id_crossref": pd.Series(dtype="str"),
                        "name_pub": pd.Series(dtype="str")
                    })

                    crossref = []
                    id_crossref = []
                    name_pub = []

                    publishers = jsondata["publishers"]
                    for key in publishers:
                        crossref.append(key)
                        id_crossref.append(publishers[key]["id"])
                        name_pub.append(publishers[key]["name"])

                    publishers_df["crossref"] = pd.Series(crossref)
                    publishers_df["id_crossref"] = pd.Series(id_crossref)
                    publishers_df["name_pub"] = pd.Series(name_pub)
                
                # =============== DATABASE CONNECTION ===============

                with sql3.connect(self.dbPath) as rdb:
                    authors_df.to_sql("AuthorsTable", rdb, if_exists="append", index=False)
                    venues_id_df.to_sql("VenuesIdTable", rdb, if_exists="append", index=False)
                    references_df.to_sql("ReferencesTable", rdb, if_exists="append", index=False)
                    publishers_df.to_sql("PublishersTable", rdb, if_exists="append", index=False)
                    rdb.commit()
            return True

class RelationalQueryProcessor(QueryProcessor,RelationalProcessor):
    def __init__(self):
        super().__init__()
    
    def getPublicationsPublishedInYear(self,year):
        if type(year) == int:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                # 2 === Also here I am missing the information regarded the cited Publications (as it does not make any sense to include it here), of which I only save the doi... therefore, I guess that in the generic we will have to retrieve all that information from the database (open a new connection?)
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalTable.id_venue==VenuesIdTable.doi_venues_id WHERE publication_year='{pub_year}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookTable.id_venue==VenuesIdTable.doi_venues_id WHERE publication_year='{pub_year}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsTable.id_venue==VenuesIdTable.doi_venues_id WHERE publication_year='{pub_year}'".format(pub_year=year)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "pub_type", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub", "venue_type"])
        else:
            raiseExceptions("The input parameter is not an integer!")
        
    def getPublicationsByAuthorId(self,id):
        if type(id) == str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                # 2 === Also here I am missing the information regarded the cited Publications (as it does not make any sense to include it here), of which I only save the doi... therefore, I guess that in the generic we will have to retrieve all that information from the database (open a new connection?)
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE orcid='{orcid}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookChapterTable.id==VenuesIdTable.doi_venues_id WHERE orcid='{orcid}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsPaperTable.id==VenuesIdTable.doi_venues_id WHERE orcid='{orcid}'".format(orcid=id)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "pub_type", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub", "venue_type"])
        else:
            raiseExceptions("The input parameter is not a string!")

    def getMostCitedPublication(self):
        with sql3.connect(self.getDbPath()) as qrdb:
            cur = qrdb.cursor()
            # 2 === Also here I am missing the information regarded the cited Publications (as it does not make any sense to include it here), of which I only save the doi... therefore, I guess that in the generic we will have to retrieve all that information from the database (open a new connection?)
            query1 = "SELECT ref_doi, COUNT(ref_doi) AS num_citations FROM ReferencesTable GROUP BY ref_doi ORDER BY num_citations DESC"
            cur.execute(query1)
            result_q1 = cur.fetchall()
            max = result_q1[0][1]
            result1 = list()
            for item in result_q1:
                index = result_q1.index(item)
                if result_q1[index][1] == max:
                    result1.append(result_q1[index][0])
            df1 = pd.DataFrame(data=result1,columns=["ref_doi"])
            query2 = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookChapterTable.id==VenuesIdTable.doi_venues_id UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsPaperTable.id==VenuesIdTable.doi_venues_id"
            cur.execute(query2)
            result_q2 = cur.fetchall()
            df2 = pd.DataFrame(data=result_q2,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "pub_type", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub", "venue_type"])
            final_result = pd.merge(left=df2, right=df1, left_on="id", right_on="ref_doi")
            qrdb.commit()
        return final_result
    
    def getMostCitedVenue(self):
        with sql3.connect(self.getDbPath()) as qrdb:
            cur = qrdb.cursor()
            query1 = "SELECT name_venue, COUNT(name_venue) as num_cit FROM JournalTable LEFT JOIN ReferencesTable ON JournalTable.id_venue==ReferencesTable.ref_doi WHERE ref_doi IS NOT NULL GROUP BY name_venue UNION SELECT name_venue, COUNT(name_venue) as num_cit FROM BookTable LEFT JOIN ReferencesTable ON BookTable.id_venue==ReferencesTable.ref_doi WHERE ref_doi IS NOT NULL GROUP BY name_venue UNION SELECT name_venue, COUNT(name_venue) as num_cit FROM ProceedingsTable LEFT JOIN ReferencesTable ON ProceedingsTable.id_venue==ReferencesTable.ref_doi WHERE ref_doi IS NOT NULL GROUP BY name_venue ORDER BY num_cit DESC"
            cur.execute(query1)
            result_q1 = cur.fetchall()
            max = result_q1[0][1]
            result1 = list()
            for item in result_q1:
                index = result_q1.index(item)
                if result_q1[index][1] == max:
                    result1.append(result_q1[index][0])
            df1 = pd.DataFrame(data=result1,columns=["name_venue"])
            query2 = "SELECT issn_isbn, NULL AS event, name_venue, publisher, name_pub, venue_type FROM VenuesIdTable LEFT JOIN JournalTable ON VenuesIdTable.doi_venues_id==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref UNION SELECT issn_isbn, NULL AS event, name_venue, publisher, name_pub, venue_type FROM VenuesIdTable LEFT JOIN BookTable ON VenuesIdTable.doi_venues_id==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref UNION SELECT issn_isbn, event, name_venue, publisher, name_pub, venue_type FROM VenuesIdTable LEFT JOIN ProceedingsTable ON VenuesIdTable.doi_venues_id==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref"
            cur.execute(query2)
            result_q2 = cur.fetchall()
            df2 = pd.DataFrame(data=result_q2, columns=["issn_isbn", "event", "name_venue", "publisher", "name_pub", "venue_type"])
            final_result = pd.merge(left=df2, right=df1, left_on="name_venue", right_on="name_venue")
            qrdb.commit()
        return final_result

    def getVenuesByPublisherId(self,id):
        if type(id) == str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT issn_isbn, NULL AS event, name_venue, publisher, name_pub, venue_type FROM VenuesIdTable LEFT JOIN JournalTable ON VenuesIdTable.doi_venues_id==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref WHERE publisher='{pub_id}' UNION SELECT issn_isbn, NULL AS event, name_venue, publisher, name_pub, venue_type FROM VenuesIdTable LEFT JOIN BookTable ON VenuesIdTable.doi_venues_id==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref WHERE publisher='{pub_id}' UNION SELECT issn_isbn, event, name_venue, publisher, name_pub, venue_type FROM VenuesIdTable LEFT JOIN ProceedingsTable ON VenuesIdTable.doi_venues_id==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref WHERE publisher='{pub_id}'".format(pub_id=id)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result, columns=["issn_isbn", "event", "name_venue", "publisher", "name_pub", "venue_type"])
        else:
            raiseExceptions("The input parameter is not string!")

    def getPublicationInVenue(self,venueId):
        if type(venueId)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE issn_isbn='{venue_id}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookChapterTable.id==VenuesIdTable.doi_venues_id WHERE issn_isbn='{venue_id}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsPaperTable.id==VenuesIdTable.doi_venues_id WHERE issn_isbn='{venue_id}'".format(venue_id=venueId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
                print(type(result))
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "pub_type", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub", "venue_type"])        
        else:
            raiseExceptions("The input parameter is not a string!")

    def getJournalArticlesInIssue(self,issue,volume,journalId):
        if type(issue)==str and type(volume)==str and type(journalId)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE issue='{issue}' AND volume='{volume}' AND issn_isbn='{journal_id}'".format(issue=issue, volume=volume, journal_id=journalId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "pub_type", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub", "venue_type"]) 
        else:
            raiseExceptions("One or all the input parameter are not strings!")

    def getJournalArticlesInVolume(self,volume,journalId):
        if type(volume)==str and type(journalId)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE volume='{volume}' AND issn_isbn='{journal_id}'".format(volume=volume, journal_id=journalId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "pub_type", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub", "venue_type"]) 
        else:
            raiseExceptions("One or all the input parameter are not strings!")

    def getJournalArticlesInJournal(self,journalId):
        if type(journalId)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE issn_isbn='{journal_id}'".format(journal_id=journalId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "pub_type", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub", "venue_type"]) 
        else:
            raiseExceptions("One or all the input parameter are not strings!")

    def getProceedingsByEvent(self,eventPartialName):
        if type(eventPartialName)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT event, name_venue, publisher, issn_isbn, name_pub, venue_type FROM ProceedingsTable LEFT JOIN VenuesIdTable ON ProceedingsTable.id_venue==VenuesIdTable.doi_venues_id LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref WHERE event LIKE '%{event}%'".format(event=eventPartialName)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["event","name_venue","publisher","issn_isbn", "name_pub", "venue_type"])
        else:
            raiseExceptions("The input parameter is not string!")

    def getPublicationAuthors(self,publicationId):
        if type(publicationId)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT doi_authors, family, given, orcid FROM AuthorsTable WHERE doi_authors='{pub_doi}'".format(pub_doi=publicationId)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["doi_authors", "family", "given", "orcid"])
        else:
            raiseExceptions("The input parameter is not string!")

    def getPublicationsByAuthorName(self,authorPartialName):
        if type(authorPartialName)==str:
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id WHERE family LIKE '%{family}%' OR given LIKE '%{given}%' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookChapterTable.id==VenuesIdTable.doi_venues_id WHERE family LIKE '%{family}%' OR given LIKE '%{given}%' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsPaperTable.id==VenuesIdTable.doi_venues_id WHERE family LIKE '%{family}%' OR given LIKE '%{given}%'".format(family=authorPartialName,given=authorPartialName)
                cur.execute(query)
                result = cur.fetchall()
                qrdb.commit()
            return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "pub_type", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub", "venue_type"]) 
        else:
            raiseExceptions("One or all the input parameter are not strings!")
            
    def getDistinctPublishersOfPublications(self,pubIdList):
        if type(pubIdList) == list and all(isinstance(n, str) for n in pubIdList):
            with sql3.connect(self.getDbPath()) as qrdb:
                cur = qrdb.cursor()
                result = list()
                for item in pubIdList:
                    query = "SELECT name_venue, id_venue, crossref, name_pub, venue_type FROM JournalTable LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref WHERE id_venue='{doi}' UNION SELECT name_venue, id_venue, crossref, name_pub, venue_type FROM BookTable LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref WHERE id_venue='{doi}' UNION SELECT name_venue, id_venue, crossref, name_pub, venue_type FROM ProceedingsTable LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref WHERE id_venue='{doi}'".format(doi = item)
                    cur.execute(query)
                    result_q = cur.fetchall()
                    result.extend(result_q)
                qrdb.commit()
            return pd.DataFrame(data=result, columns=["name_venue", "id_venue", "crossref", "name_pub", "venue_type"])
        else:
            raiseExceptions("The input parameter is not a list or it is not a list of strings!")






    # ===== ADDITIONAL GET-METHOD? FOR HANDLING THE RECURSIVE PART IN THE GENERIC ===== 
    def getPublicationsFromDOI(self,pubIdList):
            if type(pubIdList) == list and all(isinstance(n, str) for n in pubIdList):
                with sql3.connect(self.getDbPath()) as qrdb:
                    cur = qrdb.cursor()
                    result = list()
                    for item in pubIdList:
                        query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub, venue_type FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalTable.id_venue==VenuesIdTable.doi_venues_id WHERE id='{doi}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, pub_type, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub, venue_type FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookTable.id_venue==VenuesIdTable.doi_venues_id WHERE id='{doi}' UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, name_venue, issn_isbn, publisher, name_pub, venue_type FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsTable.id_venue==VenuesIdTable.doi_venues_id WHERE id='{doi}'".format(doi=item)
                        cur.execute(query)
                        result_q = cur.fetchall()
                        result.extend(result_q)
                    qrdb.commit()
                return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "pub_type", "family", "given", "orcid", "ref_doi", "name_venue", "issn_isbn", "publisher", "name_pub", "venue_type"])
            else:
                raiseExceptions("The input parameter is not a list or it is not a list of strings!") 

    def getPublications(self):
        with sql3.connect(self.getDbPath()) as qrdb:
            cur = qrdb.cursor()
            # The name_venue column contains the same information as the publication_venue column, so maybe it could be worth it to delete one of the two from the SELECT (if it is not striclty necessary)
            query = "SELECT publication_year, title, publication_venue, id, issue, volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM JournalArticleTable LEFT JOIN AuthorsTable ON JournalArticleTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN JournalTable ON ReferencesTable.og_doi==JournalTable.id_venue LEFT JOIN PublishersTable ON JournalTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON JournalArticleTable.id==VenuesIdTable.doi_venues_id UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM BookChapterTable LEFT JOIN AuthorsTable ON BookChapterTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN BookTable ON ReferencesTable.og_doi==BookTable.id_venue LEFT JOIN PublishersTable ON BookTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON BookChapterTable.id==VenuesIdTable.doi_venues_id UNION SELECT publication_year, title, publication_venue, id, NULL AS issue, NULL AS volume, NULL AS chapter_number, pub_type, family, given, orcid, ref_doi, issn_isbn, publisher, name_pub, venue_type FROM ProceedingsPaperTable LEFT JOIN AuthorsTable ON ProceedingsPaperTable.id==AuthorsTable.doi_authors LEFT JOIN ReferencesTable ON AuthorsTable.doi_authors==ReferencesTable.og_doi LEFT JOIN ProceedingsTable ON ReferencesTable.og_doi==ProceedingsTable.id_venue LEFT JOIN PublishersTable ON ProceedingsTable.publisher==PublishersTable.crossref LEFT JOIN VenuesIdTable ON ProceedingsPaperTable.id==VenuesIdTable.doi_venues_id"
            cur.execute(query)
            result = cur.fetchall()
            qrdb.commit()
        return pd.DataFrame(data=result,columns=["publication_year", "title", "publication_venue", "id", "issue", "volume", "chapter_number", "pub_type", "family", "given", "orcid", "ref_doi", "issn_isbn", "publisher", "name_pub", "venue_type"])


x = RelationalProcessor()
print("RelationalProcessor object\n", x)
x.getDbPath()
print("Here's the getDbPath result")
print(x)
x.setDbPath("SETDBPATH.db")
print(x.getDbPath())

y = RelationalDataProcessor()
print("RelationalDataProcessor object\n",y)
y.setDbPath("SETDBPATH2.db")
print(y.getDbPath())
y.uploadData("testData/relational_publications.csv")
y.uploadData("testData/relational_other_data.json")
# ======== WATCH OUT ========
# If you run the code again on here, it will upload again the .csv and the .json to the database (and therefore the getMostCited method will duplicate the result!)
# This is because of the if_exists parameter in every .to_sql("PublishersTable", rdb, if_exists="append", index=False) instruction
print("RelationalDataProcessor AFTER UPLOAD\n",y)


z = RelationalQueryProcessor()
z.setDbPath("SETDBPATH2.db")
# ===== TESTS FOR ALL THE QUERIES (FOR BOTH CASES IF-ELSE)
'''
q1 = z.getPublicationsPublishedInYear(2020)
print("getPublicationsPublishedInYear Query\n",q1)

q2 = z.getPublicationsByAuthorId("0000-0001-9857-1511")
print("getPublicationsByAuthorId Query\n",q2)

q3 = z.getMostCitedPublication()
print("getMostCitedPublication Query\n",q3)

q4 = z.getMostCitedVenue()
print("getMostCitedVenue Query\n",q4)

q5 = z.getVenuesByPublisherId("crossref:78")
print("getVenuesByPublisherId Query\n",q5)
'''
q6 = z.getPublicationInVenue("issn:0944-1344")
print("getPublicationInVenue Query\n",q6)
'''
q7 = z.getJournalArticlesInIssue("9","17","issn:2164-5515")
print("getJournalArticleInIssue Query\n",q7)

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

q14 = z.getPublicationsFromDOI(["doi:10.1080/21645515.2021.1910000"])
print("getPublicationsFromDOI Query\n",q14)
'''