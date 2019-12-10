import boto3
from boto3.dynamodb.conditions import Key, Attr
import dataParser


# Get the service resource.
dynamodb = boto3.resource('dynamodb')

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
                'AttributeName': 'Group',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'ASIN',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Group',
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
def getTable():
    productTable = dynamodb.Table('Products')
    customerTable = dynamodb.Table('Customer')
    reviewTable = dynamodb.Table('Review')

    print(productTable.creation_date_time)
    print(customerTable.creation_date_time)
    print(reviewTable.creation_date_time)

    return productTable, customerTable, reviewTable

#adding product item to product table
def createProductItem(productTable, id, ASIN, title, group, salesRank, similar, categories, avgReview):

    productTable.put_item(
        Item={
            'ID': id,
            'ASIN': ASIN,
            'Title': title,
            'Group': group,
            'SalesRank': salesRank,
            'Similar': similar, #List of co-purchased products
            'Categories': categories, #list of products categories
            'AverageReview': avgReview
        }
    )

#adding review item to review table
def createReviewItem(reviewTable, customerID, ASIN, title, rating, votes, helpful, date):
    response = reviewTable.get_item(
        Key={
            'Customer': customerID,
            'ProductASIN': ASIN,
            'Title': title,
            'Rating': rating,
            'Votes': votes,
            'Helpful': helpful,
            'Date': date,
        }
    )
    review = response['Item']

    return review

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
def queryProductASIN(productTable, ASIN):
    response = productTable.query(
        KeyConditionExpression=Key('ASIN').eq(ASIN)
    )
    items = response['Items']
    return items

#gets the average review rating for a product given it's ASIN
def queryProductAvgReview(productTable, ASIN, averageReview):
    response = productTable.scan(
        FilterExpression=Key('ASIN').eq(ASIN),
        ProjectionExpression='Title, AverageReview'
    )
    items = response['Items']
    return items

#get the products that were co-purchased along with this product
def querySimilarProducts(productTable, ASIN):
    response = productTable.scan(
        FilterExpression=Key('ASIN').eq(ASIN),
        ProjectionExpression='Title, Similar'
    )
    items = response['Items']
    return items

#get all of the products given a group
def queryProductsOfGroup(productTable, group):
    response = productTable.scan(
        FilterExpression=Key('Group').eq(group),
        ProjectionExpression='Title'
    )
    items = response['Items']
    return items

#get all of the products given a category
def queryProductsOfCategory(productTable, category):
    response = productTable.scan(
        FilterExpression=Key('Category').contains(category),
        ProjectionExpression='Title'
    )
    items = response['Items']
    return items

#get all of the review data given a product ASIN and a customerID
def queryReviewData(reviewTable, ASIN, customerID):
    response = reviewTable.query(
        KeyConditionExpression=Key('ASIN').eq(ASIN) & Key('CustomerID').eq(customerID)
    )
    items = response['Items']
    return items

#products with reviews from minRating to maxRating for product with given ASIN
def queryReviewRatingBtwn(reviewTable, productTable, ASIN, minRating, maxRating):

    #find all of the reviews with ratings from minRating to maxRating
    response = reviewTable.scan(
        FilterExpression=Key('ASIN').eq(ASIN) & Key('rating').between(minRating, maxRating),
        ProjectionExpression='Title'
    )
    items = response['Items']
    return items

#find all the reviews for a given product
def queryAllReivews(reviewTable, ASIN):
    response = reviewTable.query(
        KeyConditionExpression=Key('ASIN').eq(ASIN)
    )
    items = response['Items']
    return items

#find all the reviews for a given customer
def queryCustomerReivews(reviewTable, customerID):
    response = reviewTable.query(
        KeyConditionExpression=Key('CustomerID').eq(customerID)
    )
    items = response['Items']
    return items

#find reviews that have a helpful rating greater than a given one
def queryHelpfulGreater(reviewTable, ASIN, helpful):
    response = reviewTable.scan(
        FilterExpression=Key('ASIN').eq(ASIN) & Key('Helpful').gt(helpful),
        ProjectionExpression='Title'
    )
    items = response['Items']
    return items

#find all reviews that have a helpful rating greater than a given one
def queryVotesGreater(reviewTable, ASIN, votes):
    response = reviewTable.query(
        KeyConditionExpression=Key('ProductASIN').eq(ASIN) & Key('Votes').eq(votes)
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
def getData(productTable, reviewTable):
    categories = []  # list of categories
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
    products = []
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
            numCategoires = line[12:].strip()
            numC = int(numCategoires)
            for i in range(numC):
                line = f.readline()
                category = []
                category = line.strip().split('|')
                categories.append(category)
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
            categories.clear()
            reviews.clear()
        elif line == "" and dontAddProduct == 0:
            products.append(product)

            createProductItem(productTable, product['Id'], product['ASIN'], product['title'], product['group'],
                              product['salesRank'], product['similar'], product['categories'],
                              product['reviewInfo']['avgRating'])


            rCount = product["reviewInfo"]["total"]
            reviews_ = product["reviews"]

            for i in range(int(rCount)):
                createReviewItem(reviewTable, reviews["customer"], product["ASIN"], reviews["title"],
                                 reviews["rating"], reviews["votes"], reviews["helpful"], reviews["date"])

            product.clear()
            categories.clear()
            reviews.clear()


def main():
    productTable, reviewTable = createTables()

    #getData(productTable, reviewTable)


main()

"""  MYSQL PORTION

PART 1

Create Tables on SQL Server

Extract Data

Put data on the server


PART 2

Define SQL queries that we want to run

Use server api to extract data based on supplied SQL queries

Get the data and present it to console

"""








""" Co-purchasing recommendation system

Create a graph on a data storage server

Put data on the graph having similar products be edges

Have edge weights be similarity of each of the products *

Compute the degree of centrality of a node and store it in the node ** 

Determine the top5 recommendations based on something

* need to decide what algorithm to use 

** need to cite the medium article in the paper


"""