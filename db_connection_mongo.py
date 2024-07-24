#-------------------------------------------------------------------------
# AUTHOR: Breanne Ha
# FILENAME: Assignment2
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from collections import defaultdict
from pymongo import MongoClient
import datetime

def connectDataBase():

    DB_Name = "Assignment2"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:
        
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_Name]

        return db
    
    except:
        print("No database connection")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    doc = {"_id": docId,
           "title": docTitle,
           "text": docText,
           "date": docDate,
           "cat": docCat,
           }
    
    col.insert_one(doc)

def deleteDocument(col, docId):

    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    doc = {"$set": {"text": docText, "title": docTitle, "date": docDate, "cat": docCat}}

    col.update_one({"_id": docId}, doc)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}

    term_counts = {}

    # MongoDB aggregation pipeline to group by term and document_title and count occurrences
    pipeline = [
        {"$match": {"docTitle": col}},  # Match documents for a specific document title
        {"$group": {"_id": "$term", "count": {"$sum": 1}}}  # Group by term and count occurrences
    ]

    results = col.aggregate(pipeline)

    # Process results and store in term_counts dictionary
    for result in results:
        term = result["_id"]
        count = result["count"]
        term_counts[term] = f'{col}:{count}'

    return term_counts
    



