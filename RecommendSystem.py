
"""

Addisalem Kebede
Big Data - Final Project

Co-purchasing recommendation system

Create a graph on a data storage server

Put data on the graph having similar (copurchased) products be edges

Have edge weights be similarity of each of the products *

Determine the top5 recommendations based on co-purchased data

* need to decide what algorithm to use

"""

#Use the networkx library

import networkx as nx
import math


#open file and then parse data into what we are looking for
def getData():

    categories = ""  # list of categories
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

    products = {}

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
            categories = []
            for i in range(numC):
                line = f.readline()
                category = []
                category = line.strip().split('|')
                for j in range(len(category)):
                    temp = category[j].split('[')
                    category[j] = temp[0]
                categories = categories + category[1:]   # categories + line.strip() #categories + category[1:]
                categories = list(dict.fromkeys(categories)) #gets rid of duplicates in the list
            product["categories"] = categories

        elif line.startswith('reviews:'):
            reviewInfo = line.split()
            product["reviewInfo"] = {}
            product["reviewInfo"]["total"] = reviewInfo[2]
            product["reviewInfo"]["downloaded"] = reviewInfo[4]
            product["reviewInfo"]["avgRating"] = reviewInfo[7]
            reviews.clear()
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

        elif line == "" and dontAddProduct == 0:
            products[product["ASIN"]] = {"Id": product["Id"],
                                         "ASIN": product["ASIN"],
                                         "title": product["title"],
                                         "group": product["group"],
                                         "salesrank": product["salesrank"],
                                         "similar": product["similar"],  # products that were co-purchased
                                         "categories": product["categories"],  # list of categories
                                         "reviews": product["reviews"],  # dictionary of reviews
                                         "reviewInfo": product["reviewInfo"],
                                         }
            #print(product["similar"])
            product.clear()


    return products

# get rid of products that have no co-purchased products
def cleanData(products):
    delete = [key for key in products if len(products[key]["similar"]) == 0]
    for key in delete: del products[key]

    # get rid of co purchased products that don't exist as products in dictionary.
    for key in products.keys():
        finalCopurchased = []
        for copurchased in products[key]["similar"]:
            if copurchased in products.keys():
                finalCopurchased.append(copurchased)
        products[key]["similar"] = finalCopurchased

    return products

# add products to graph with edges going to co purchased products and
# edge weights being the cosine similarity of the categories of the two products
def addToGraph(products):
    graph = nx.Graph()

    # Add nodes to graph and calculate the cosine similarity of each node relative to the category
    # and have that be the weight of the edges
    # https://www.machinelearningplus.com/nlp/cosine-similarity/
    for key in products.keys():
        graph.add_node(key)

        # get the list of categories associated with product as well as products that were co purchased with this product

        productCategories = products[key]["categories"]

        for coPurchased in products[key]["similar"]:
            #print(key)
            coPurchasedCategories = products[coPurchased]["categories"]

            #print("CO CATEGORIES:", coPurchasedCategories)
            #print("PROD CATEGORIES:", productCategories)

            cosineSim = getCosineSimilarity(set(coPurchasedCategories), set(productCategories))

            #print("similarity: ", cosineSim)

            graph.add_edge(key, coPurchased, weight=cosineSim)

    return graph

#compute the cosine similarity of two products given the categories
def getCosineSimilarity(set1, set2):

    #union the sets to see all possible categories
    setUnion = set1.union(set2)

    #for each value in the unioned set check to see if it exists in set1 or 2
    # if it does then place a 1 for list if not then put a 0
    list1 = []
    list2 = []
    for category in setUnion:

        if category in set1:
            list1.append(1)
        else:
            list1.append(0)
        if category in set2:
            list2.append(1)
        else:
            list2.append(0)


    #print("List 1 :",list1)
    #print("List 2 :",list2)

    #compute dot product of lists
    dot = 0
    for i in range(len(setUnion)):
        dot = dot + list1[i] * list2[i]

    #print("DOT: ", dot)
    #compute cosine similarity
    magnitude1 = math.sqrt(math.pow(sum(list1), 2))
    magnitude2 = math.sqrt(math.pow(sum(list2), 2))

    #print("MAG 1: ", magnitude1, "MAG2: ", magnitude2)

    sim = dot / math.sqrt(magnitude1 * magnitude2)

    return sim

# make recommendations based on copurchased items and similarity of their categories
# determine the order of similarity and return the ASINS of similar items
def makeRecommendation(graph, products, ASIN):
    edgesDict = graph[ASIN]
    recomendation = []
    items = {}
    for key in edgesDict.keys():
        items[key] = edgesDict[key]["weight"]

    items = sorted(items.items(), key =lambda kv:(kv[1], kv[0]))


    print("The recommendations based on the item with ASIN (",ASIN,") are: ")
    for item in items:
        product = products[item[0]]
        print(product["title"])


products = getData() # gets a dictionary of all products with the ASIN as the key and a product info dictionary as the value
products = cleanData(products)
graph = addToGraph(products)
makeRecommendation(graph, products, "0827229534")



