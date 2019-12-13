"""
Addisalem Kebede
Big Data - Final Project

Connect to Amazon's DynamoDB

Create tables

Parse data

Add data to tables

Query data using preset queries from tables

"""

import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import *

# need to run the following command in the directory where dynamodb was downloaded to have dynamodb working locally
# aws dynamodb describe-table --table-name Review --endpoint-url http://localhost:8000

# Get the service resource.
dynamodb = boto3.resource('dynamodb',
                          endpoint_url='http://localhost:8000',
                          aws_access_key_id='keyid',
                          aws_secret_access_key='secret',
                          verify=False)


#not able to upload to cload given permission issues when trying to create access key.
#If wanted to do so just create a user with an access key on AWS server and use that
#  access key here with the endpoint_url being "us-west-2". This is the closest server to Washington


def createTables():

    # Create the DynamoDB table.
    productTable = dynamodb.create_table(
        TableName='Products',
        KeySchema=[
            {
                'AttributeName': 'ASIN',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'GroupName',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'ASIN',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'GroupName',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    reviewTable = dynamodb.create_table(
        TableName='Review',
        KeySchema=[
            {
                'AttributeName': 'ASIN',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'customerID',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'ASIN',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'customerID',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Wait until the table exists.
    productTable.meta.client.get_waiter('table_exists').wait(TableName='Products')
    reviewTable.meta.client.get_waiter('table_exists').wait(TableName='Review')

    # Print out some data about the table.
    print(productTable.item_count)
    print(reviewTable.item_count)

    return productTable, reviewTable

#Get an existing table
def getTables():
    productTable = dynamodb.Table('Products')
    reviewTable = dynamodb.Table('Review')

    print(productTable.creation_date_time)
    print(reviewTable.creation_date_time)

    return productTable, reviewTable

#adding product item to product table
def createProductItem(productTable, id, ASIN, title, group, salesRank, similar, categories, avgReview, totalReviews):

    productTable.put_item(
        Item={
            'ASIN': ASIN,
            'GroupName': group,
            'ID': id,
            'Title': title,
            'SalesRank': int(salesRank),
            'SimilarProducts': similar, #List of co-purchased products
            'Categories': categories, #list of products categories
            'AverageReview': Decimal(avgReview),
            'totalReviews' : Decimal(totalReviews)
        }
    )

#adding review item to review table
def createReviewItem(reviewTable, customerID, ASIN, title, rating, votes, helpful, date):
     reviewTable.put_item(
        Item={
            'customerID': customerID,
            'ASIN': ASIN,
            'Title': title,
            'Rating': int(rating),
            'Votes': int(votes),
            'Helpful': int(helpful),
            'Date': date,
        }
    )

#getting product item from product table
def getProductItem(productTable, ASIN):
    response = productTable.get_item(
        Key={
            'ASIN': ASIN
        }
    )
    product = response['Item']

    return product

#getting product item from product table
def getReviewItem(customerTable, ASIN, customerID):
    response = customerTable.get_item(
        Key={
            'ProductASIN': ASIN,
            'CustomerID': customerID
        }
    )
    product = response['Item']
    return product

######################### QUERIES #########################

#getting a product using its ASIN
def queryProduct(productTable, Title):
    response = productTable.scan(
        FilterExpression=Attr('Title').contains(Title)
    )
    items = response['Items']
    return items

#gets the average review rating for a product given it's ASIN
def queryProductAvgReview(productTable, title):
    response = productTable.scan(
        FilterExpression=Attr('Title').contains(title),
        ProjectionExpression='Title, AverageReview'
    )
    items = response['Items']
    return items

#get the products that were co-purchased along with this product
def querySimilarProducts(productTable, title):
    response = productTable.scan(
        FilterExpression=Attr('Title').contains(title),
        ProjectionExpression='Title, SimilarProducts'
    )
    items = response['Items']
    return items

#get all of the products given a group
def queryProductsOfGroup(productTable, group):
    response = productTable.scan(
        FilterExpression=Key('GroupName').eq(group),
        ProjectionExpression='Title'
    )
    items = response['Items']
    return items

#get all of the products given a category
def queryProductsOfCategory(productTable, category):
    response = productTable.scan(
        FilterExpression= Attr('Categories').contains(category),
        ProjectionExpression='Title'
    )
    items = response['Items']
    return items

#products with reviews from minRating to maxRating for product with given ASIN
def queryReviewRatingBtwn(productTable, minRating, maxRating):

    #find all of the reviews with ratings from minRating to maxRating
    response = productTable.scan(
        FilterExpression=Key('AverageReview').between(minRating, maxRating),
        ProjectionExpression='Title, AverageReview'
    )
    items = response['Items']
    return items

#find all the reviews for a given product
def queryAllReivews(reviewTable, title):
    response = reviewTable.scan(
        FilterExpression=Attr('Title').contains(title)
    )
    items = response['Items']
    return items

#find all the reviews for a given customer
def queryCustomerReivews(reviewTable, customerID):
    response = reviewTable.scan(
        FilterExpression=Key('customerID').eq(customerID)
    )
    items = response['Items']
    return items

#find reviews that have a helpful rating greater than a given one
def queryHelpfulGreater(reviewTable, title, helpful):
    response = reviewTable.scan(
        FilterExpression=Attr('Title').contains(title) & Key('Helpful').gt(helpful)
    )
    items = response['Items']
    return items

#find all reviews that have a votes rating greater than a given one
def queryVotesGreater(reviewTable, title, votes):
    response = reviewTable.scan(
        FilterExpression=Attr('Title').contains(title) & Key('Votes').gt(votes),
        ProjectionExpression='Title, customerID, Votes'
    )
    items = response['Items']
    return items

#find all of the titles that have the given substring
def queryTitle(productTable, title):
    response = productTable.scan(
        FilterExpression=Attr('Title').contains(title),
        ProjectionExpression='Title'
    )
    items = response['Items']
    return items

#find all prodcuts of a group with an average rating greater than x
def queryAvgRatingGroupGT(productTable, group, rating):
    response = productTable.scan(
        FilterExpression=Key('GroupName').eq(group) & Key('AverageReview').gt(rating),
        ProjectionExpression='Title, AverageReview, GroupName'
    )
    items = response['Items']
    return items


"""
Possible QUERIES

Product:
Find all product info given product title or ID or ASIN 
Find average review of a given product
Find all co-purchased products of a given product
Find all products of a given group
Find all products that have a given category


Review:
Find a specific review based on the product and customer
Find reviews with ratings between x and y
Find reviews from a given customer
Find reviews for a given product

Find reviews for a product where the helpful rating is more than x ----- why???
Find reviews for a product where it's votes are greater than x
"""


#open file and then parse data into what we are looking for
def parseAndInsert(productTable, reviewTable):
    categories = "" #[]  # list of categories
    reviews = [] # dictionary of reviews
    product = {
        "Id": "",
        "ASIN": "",
        "title": "",
        "group": "",
        "salesrank": "",
        "similar": [],  # products that were co-purchased
        "categories": [],  # list of categories
        "reviews": {},  # dictionary of reviews
        "reviewInfo": {},
        "customers": []  # list of customers for a given product
    }
    dontAddProduct = 0 #0 -> add product 1-> dont add products
    f = open("data.txt", "r")
    for line in f:
        i = 0
        line = line.strip()
        if line.startswith('Id'):
            product["Id"] = line[6:]
        elif line.startswith('ASIN'):
            product["ASIN"] = line[6:]
        elif line.startswith('title'):
            product["title"] = line[7:]
            dontAddProduct = 0
        elif line.startswith('group'):
            product["group"] = line[7:]
        elif line.startswith('salesrank'):
            product["salesrank"]  = line[11:]
        elif line.startswith('similar'):
            product["similar"] = line[12:].split()
        elif line.startswith('categories'):
            numCategories = line[12:].strip()
            numC = int(numCategories)
            categories = ""
            for i in range(numC):
                line = f.readline()
                #category = []
                #category = line.strip().split('|')
                categories = categories + line.strip()#categories + category[1:]
            product["categories"] = categories
        elif line.startswith('reviews:'):
            reviewInfo = line.split()
            product["reviewInfo"] = {}
            product["reviewInfo"]["total"] = reviewInfo[2]
            product["reviewInfo"]["downloaded"] = reviewInfo[4]
            product["reviewInfo"]["avgRating"] = reviewInfo[7]
            numR = int(reviewInfo[2])
            for i in range(numR):
                line = f.readline()
                reviewData= line.strip().split()
                review = {}
                review["date"] = reviewData[0]
                review["customer"] = reviewData[2]
                review["rating"] = reviewData[4]
                review["votes"] = reviewData[6]
                review["helpful"] = reviewData[8]
                reviews.append(review)
            product["reviews"] = reviews
        elif line.startswith('discontinued'): #dont consider discontinued products
            dontAddProduct = 1
            product.clear()
            #categories.clear()
            reviews.clear()
        elif line == "" and dontAddProduct == 0:
            avgRating = product['reviewInfo']['avgRating']
            Id = product['Id']
            ASIN = product['ASIN']
            title =  product['title']
            group =  product['group']
            salesrank =  product['salesrank']
            similar = product['similar']  # products that were co-purchased
            categories = product['categories']  # list of categories
            totalReviews = product['reviewInfo']['total']

            if len(categories) == 0: categories = "none"

            createProductItem(productTable, Id, ASIN, title, group,
                              salesrank, similar, categories,
                              avgRating, totalReviews)


            rCount = product["reviewInfo"]["total"]
            pReviews = product["reviews"]

            for i in range(int(rCount)):
                createReviewItem(reviewTable, pReviews[i]["customer"], product["ASIN"], product["title"],
                                 pReviews[i]["rating"], pReviews[i]["votes"], pReviews[i]["helpful"], pReviews[i]["date"])

            product.clear()
            #categories.clear()
            reviews.clear()

def main():
    productTable, reviewTable = createTables()
    productTable, reviewTable = getTables()

    parseAndInsert(productTable, reviewTable)

    done =1
    while(done == 1):
        query = menu()
        result = []
        if query == 1:
            print("\nQuery all reviews for a product.")
            title = input("Provide a Title or substring of a Title: ")
            result = queryAllReivews(reviewTable, title)
            print("\n\nReviews for", title,":\n\n")
            print("CustomerID | Title | Rating | Votes | Helpful | Date")
            for item in result:
                print(item["customerID"], "| ", item["Title"], "| ", item["Rating"], "| ",
                      item["Votes"], "| ", item["Helpful"], "| ", item["Date"])

        elif query == 2:
            print("\nQuery all reviews for a given customer.")
            cID = input("Provide the customerID: ")
            result = queryCustomerReivews(reviewTable, cID)
            print("\n\nReviews by customer with customerID", cID,":\n\n")
            print("CustomerID | Title | Rating | Votes | Helpful | Date")
            for item in result:
                print(item["customerID"], "| ", item["Title"], "| ", item["Rating"], "| ",
                      item["Votes"], "| ", item["Helpful"], "| ", item["Date"])

        elif query == 3:
            print("\nQuery product information.")
            title = input("Provide a Title or substring of a Title: ")
            result = queryProduct(productTable, title)
            print("\n\n Info about products with title containing", title,":\n\n")
            print("Title | Group | ID | Sales Rank | Co-Purchased Products | Average Rating | ASIN")
            for item in result:
                print(item["Title"], "| ", item["GroupName"], "| ", item["ID"], "| ", item["SalesRank"], "| ",
                      item["SimilarProducts"],  "| ", item["AverageReview"], "| ",
                      item["ASIN"], "| ", item['totalReviews']) # "| ", item["Categories"],

        elif query == 4:
            print("\nQuery reviews for a product that have a helpful score > min score.")
            title = input("Provide a Title or substring of a Title: ")
            minScore = int(input("Provide a a min Helpful Score: "))
            result = queryHelpfulGreater(reviewTable, title, minScore) #-- Works
            print("\n\n Products with", title,"in title have a helpful score of:\n\n")
            print("CustomerID | Title | Rating | Votes | Helpful | Date")
            for item in result:
                print(item["customerID"], "| ", item["Title"], "| ", item["Rating"], "| ",
                      item["Votes"], "| ", item["Helpful"], "| ", item["Date"])

        elif query == 5:
            print("\nQuery the average review of a product.")
            title = input("Provide a Title or substring of a Title: ")
            result = queryProductAvgReview(productTable, title) #-- Works
            print("\n\n Products with", title, "in title have an average rating of:\n\n")
            print("Title | Rating")
            for item in result:
                print(item["Title"], "| ", item["AverageReview"])

        elif query == 6:
            print("\nQuery products of a category.")
            category = input("Provide a Category: ")
            result = queryProductsOfCategory(productTable, category) #-- works
            print("\n\nAll products that have", category, "as a category:\n\n")
            print("Product Title")
            for item in result:
                print(item["Title"])

        elif query == 7:
            print("\nQuery co-purchased products.")
            title = input("Provide a Title or substring of a Title: ")
            result = querySimilarProducts(productTable, title) #--works
            print("\n\nAll ASIN values of products that are similar to", title, ":\n\n")
            print("Product Title | Similar Item ASIN values")
            for item in result:
                print(item["Title"], "| ", item["similar"])

        elif query == 8:
            print("\nQuery reviews for a product that has a votes score > min votes.")
            title = input("Provide a Title or substring of a Title: ")
            voteScore = int(input("Provide a a min votes score: "))
            result = queryVotesGreater(reviewTable, title, voteScore) #-- works
            print("\n\nAll reviews of product", title, "that have a vote score greater than", voteScore, ":\n\n")
            print("CustomerID | Votes")
            for item in result:
                print(item["customerID"], "| ", item["Votes"])

        elif query == 9:
            print("\nQuery products of a group.")
            group = input("Provide a Group: ")
            result = queryProductsOfGroup(productTable, group) #-- works
            print("\n\nAll products that are part of the", group, "group:\n\n")
            print("Product Title")
            for item in result:
                print(item["Title"])


        elif query == 10:
            print("\nQuery Titles.")
            title = input("Provide a Title or substring of a Title: ")
            result = queryTitle(productTable, title) #-- works
            print("\n\nAll products that have substring", title, "in their name:\n\n")
            print("Product Title")
            for item in result:
                print(item["Title"])

        elif query == 11:
            print("\nQuery products with average review rating between min rating and max rating.")
            minRating = int(input("Provide a a min rating: "))
            maxRating = int(input("Provide a a max rating: "))

            result = queryReviewRatingBtwn(productTable, minRating, maxRating)
            print("\n\nAll products that have a average rating between", minRating, "and", maxRating, ":\n\n")
            print("Product Title | Average Rating")
            for item in result:
                print(item["Title"], "| ", item["AverageReview"])

            #print(result)

        elif query == 12:
            print("\nQuery the products that are part of a group with an average rating > min rating")
            group = input("Provide a Group: ")
            minRating = int(input("Provide a a min rating: "))
            result = queryAvgRatingGroupGT(productTable, group, minRating)
            print("\n\nProducts inside of the", group, "group with average rating above", minRating, ":\n\n")
            print("Product Title | Average Rating")
            for item in result:
                print(item["Title"], "| ", item["AverageReview"])

        else:
            print("That is not an acceptable query selection")

        done = int(input("\n\nWould you like to run another query? \n1 - Yes | 2 - No: "))

    productTable.delete()
    reviewTable.delete()

def menu():

    print("***** Hello! Welcome to the Amazon Product Query Engine  *****")
    print("Select one of the following queries and input the requested information:")

    print("    1. Query all reviews for a product.")
    print("         Inputs: Substring of product title.") # get the substring

    print("    2. Query all reviews for a given customer.")
    print("         Inputs: customer Id. ") # get the customer Id

    print("    3. Query product information.")
    print("         Inputs: Substring of product title.") # get the title substring

    print("    4. Query reviews for a product that have a helpful score > min score.")
    print("         Inputs: Substring of product title, min score")  # get the title substring, and score

    print("    5. Query the average review of a product.")
    print("         Inputs: Substring of product title.")  # get the title substring

    print("    6. Query products of a category.")
    print("         Inputs: Category.")  # get the category for querying

    print("    7. Query co-purchased products.")
    print("         Inputs: Substring of product title.")  # get the title substring

    print("    8. Query reviews for a product that has a votes score > min votes.")
    print("         Inputs: Substring of product title, min votes.")  # get the title substring

    print("    9. Query products of a group.")
    print("         Inputs: Group.")  # get the title substring

    print("    10. Query Titles.")
    print("         Inputs: Substring of a product title.")  # gets all of the titles substring with given substring

    print("    11. Query products with average review rating between min rating and max rating.")
    print("         Inputs: Min rating, max rating.")  # get the title substring

    print("    12. Query the products that are part of a group with an average rating > min rating")
    print("         Inputs: Group, min rating.")  # get the title substring

    q = int(input("Which query do you want to preform (1-12): "))

    return q

main()

"""  MYSQL PORTION

PART 1

Create Tables on SQL Server -- Done

Extract Data -- Done

Put data on the local server -- Done

Put data on the Web server

PART 2

Define SQL queries that we want to run

Use server api to extract data based on supplied SQL queries

Get the data and present it to console

"""







