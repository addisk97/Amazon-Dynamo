



#open file and then parse data into what we are looking for
def getData():

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
            product.clear()
            categories.clear()
            reviews.clear()

getData()