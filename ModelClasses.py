# Implementation of the UML data model via Python classes

'''
class <class name>(<superclass 1>,<superclass 2>, ...):
    def __init__(self, <param 1>, <param 2>, ...)   -> constructor of an object of that class

! All the methods of a class, including its constructor __init__, MUST specify "self" as the first parameter !

! In Python relations can be represented as the other attributes (e.g. by assigning some specific values to self-declared variables) !


'''


class IdentifiableEntity(object):
    # -- Constructor
    def __init__(self, identifiers):
        self.id = set() # Constraint is [1..*], hence the set
        for identifier in identifiers:
            self.id.add(identifier)

    # -- Methods
    def getIds(self):
        result = list()
        for ids in self.id:
            result.append(ids)
        return result


""" id1 = IdentifiableEntity(["10546"])
print(id1)
print(type(id1))  
print(id1.getIds())

print("\n------------------------\n")

id2 = IdentifiableEntity(["10546","46351"])
print("This is id2 \n",id2)
print(type(id2))  
print(id2.getIds())
"""



class Person(IdentifiableEntity):
    # -- Constructor
    def __init__(self, givenName, familyName, identifiers):
        self.givenName = givenName  # Must be exactly 1 string
        self.familyName = familyName  # Must be exactly 1 string

        # --- Upperclass parameters
        super().__init__(identifiers)

    # -- Methods
    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName


# person1 = Person("Ahsan","Syed",["Ahsa98"])
""" print("This is person1",person1)
print(type(person1))
print("This is the familyName of person1\n",person1.getFamilyName())
print("This is the givenName of person1\n",person1.getGivenName())
print(person1.getIds())
"""
# person2 = Person("Francesca","Budel",["FraB99","Fra99"])

class Organization(IdentifiableEntity):
    # -- Constructor
    def __init__(self, name, identifiers):
        self.name = name

        # --- Upperclass parameters
        super().__init__(identifiers)

    # -- Methods
    def getName(self):
        return self.name


# org1 = Organization("The Belmeloro organization",["belm2023"])
"""
print(org1)
print(type(org1))
print("This is the name of org1\n",org1.getName())
print(org1.getIds()) """


class Venue(IdentifiableEntity):
    # -- Constructor
    def __init__(self, title, identifiers, publisher):
        self.title = title

        # --- Relations
        self.publisher = publisher

        # --- Upperclass parameters
        super().__init__(identifiers)

    # -- Methods
    def getTitle(self):
        return self.title

    def getPublisher(self):  # Returns an Organization object
        return self.publisher


# ven1 = Venue("Belmeloro Venue", ["belm0000", "belm1111"], org1)
""" 
print(ven1)
print(type(ven1))

print("This is the title of the venue\n",ven1.getTitle())
print("This is the publisher of the venue",ven1.getPublisher())
print("This is the ID of the venue", ven1.getIds())

print("------------------------------------ \n ------------------------------")

print("ID OF THE PUBLISHER\n",ven1.getPublisher().getIds())
print("NAME OF THE PUBLISHER\n",ven1.getPublisher().getName()) """


class Publication(IdentifiableEntity):
    # -- Constructor
    def __init__(self, publicationYear, title, identifiers, publicationVenue, author, cites):
        self.publicationYear = publicationYear
        self.title = title

        # --- Relations
        self.publicationVenue = publicationVenue
        self.author = author  # This can be an input list/set as the relation has constraints 1..*
        self.cites = cites  # Input list/set, as constraint is 0..*

        # --- Upperclass parameters
        super().__init__(identifiers)

    # -- Methods
    def getPublicationYear(self):
        return self.publicationYear

    def getTitle(self):
        return self.title

    def getCitedPublications(self):
        return self.cites

    def getPublicationVenue(self):
        return self.publicationVenue

    def getAuthors(self):
        result = set()
        for person in self.author:
            result.add(person)
        return result

# pub1 = Publication(1963, "The Name of the Rose", ["abc1001","cba1001"], ven1,[person1,person2],[])
""" print(pub1)
print(type(pub1))
print("This is the publication year of the publication\n",pub1.getPublicationYear())
print("This is the title of the publication",pub1.getTitle())
print("This is the cited publications of the publication", pub1.getCitedPublications())
print("This is the publication venue of the publication",pub1.getPublicationVenue())
print("This is the authors of the publication",pub1.getAuthors())
"""

class JournalArticle(Publication):
    # -- Constructor
    def __init__(self, issue, volume, publicationYear, title, identifiers, publicationVenue, author, cites):
        self.issue = issue
        self.volume = volume

        # --- Upperclass parameters
        super().__init__(publicationYear, title, identifiers, publicationVenue, author, cites)
    
    # -- Methods
    def getIssue(self):
        return self.issue
    
    def getVolume(self):
        return self.volume
    
# journal_article1 = JournalArticle("issue1","volume1",1944,"Journal Article on WW2",["id1","id2"],ven1,[person2],[pub1])
""" print(journal_article1)
print(type(journal_article1))
print(journal_article1.getIssue())
print(journal_article1.getVolume())

print(journal_article1.getAuthors())
print(journal_article1.getCitedPublications())
print(journal_article1.getIds())
print(journal_article1.getPublicationYear())
print(journal_article1.getTitle())
print(journal_article1.getPublicationVenue())
 """

class BookChapter(Publication):
    # -- Constructor
    def __init__(self, chapterNumber, publicationYear, title, identifiers, publicationVenue, author, cites):
        self.chapterNumber = chapterNumber

        # --- Upperclass parameters
        super().__init__(publicationYear, title, identifiers, publicationVenue, author, cites)
    
    # -- Methods
    def getChapterNumber(self):
        return self.chapterNumber

""" book_chapter1 = BookChapter(1,1944,"Book on WW2",["id11","id12"],ven1,[person1],[pub1])
print(book_chapter1)
print(type(book_chapter1))
print(book_chapter1.getChapterNumber())

print(book_chapter1.getAuthors())
print(book_chapter1.getCitedPublications())
print(book_chapter1.getIds())
print(book_chapter1.getPublicationYear())
print(book_chapter1.getTitle())
print(book_chapter1.getPublicationVenue())

print("--------------------- \n ------------------- \n -----------------") """

class ProceedingsPaper(Publication):
    pass

""" proceedings_paper1 = ProceedingsPaper(1963, "The Name of the Rose", ["abc1001","cba1001"], ven1,[person1,person2],[])
print(proceedings_paper1)
print(type(proceedings_paper1))

print("--------------------- \n ------------------- \n -----------------") """

class Journal(Venue):
    pass

""" journal1 = Journal("Belmeloro Venue", ["belm0000", "belm1111"], org1)
print(journal1)
print(type(journal1))

print("--------------------- \n ------------------- \n -----------------") """

class Book(Venue):
    pass

""" book1 = Book("Belmeloro Venue", ["belm0000", "belm1111"], org1)
print(book1)
print(type(book1))

print("--------------------- \n ------------------- \n -----------------") """

class Proceedings(Venue):
    # -- Constructor
    def __init__(self, event, title, identifiers, publisher):
        self.event = event

        # --- Upperclass parameters
        super().__init__(title, identifiers, publisher)
    
    def getEvent(self):
        return self.event
    
""" proceedings1 = Proceedings("Event Belmeloro","Belmeloro Venue", ["belm0000", "belm1111"], org1)
print(proceedings1)
print(type(proceedings1))
print(proceedings1.getEvent()) """

class QueryProcessor(object):
    pass