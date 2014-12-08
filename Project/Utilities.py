__author__ = 'aravindh'

########################################################################################################################
# This file has small utility functions to do the following:
# 1. combine_files(): This is used to combine data from two collections - restaurants (subset of business JSON file )
#                     and reviews. The output is written to RestaurantReviews.json
# 2. get_reviews(): This utility is used to write the business_id and reviews from reviews collection to the file
#                   BusinessReviews.json
# 3. get_training_reviews(): This utility function is used to generate a subset of training reviews from the main
#                            reviews collection. Set the size_of_training_review to the required count.
# 4. gen_review_coll_with_id(): This utility function is used to add a new counter field (record id) to a collection
########################################################################################################################

from pymongo import MongoClient
import json

# Combine restaurant and review collection


class Business:
    def __init__(self, business_id, text, category):
        self.text = text
        self.category = category
        self.business_id = business_id


def combine_files():
    restaurant_collection = MongoClient('localhost', 29017).yelp.restaurants
    review2_collection = MongoClient('localhost', 29017).yelp.review2
    output_file = open("RestaurantReviews.json", 'w')
    cursor = restaurant_collection.find()
    line = 0
    for entry in cursor:
        business_id = entry["business_id"]
        category = entry["categories"]
        review2_cursor = review2_collection.find({"business_id": business_id})
        review_text = ""
        for business_entry in review2_cursor:
            review_text = review_text + business_entry["text"]
            # print json.dumps(vars(obj))
        if review_text:
            line += 1
            obj = Business(business_id, review_text, category)
            output_file.write(json.dumps(vars(obj)))
            output_file.write("\n")
        if line % 100 == 0:
            print line
    output_file.close()


# Get the Business ID and Review Text


class Review:
    def __init__(self, business_id, text):
        self.business_id = business_id
        self.text = text


def get_reviews():
    review_collection = MongoClient('localhost', 29017).yelp.review2
    review_cursor = review_collection.find()
    output_file = open("BusinessReviews.json", "w")
    line = 0
    for entry in review_cursor:
        business_id = entry["business_id"]
        text = entry["text"]
        if text:
            line += 1
            obj = Review(business_id, text)
            output_file.write(json.dumps(vars(obj)))
            output_file.write("\n")
        if line % 100 == 0:
            print line


# Create the training dataset in the form {text:"", label:""}


class TrainReview:
    def __init__(self, text, label):
        self.text = text
        self.label = label


def get_training_reviews(size_of_training_review=None):
    review_collection = MongoClient('localhost', 29017).yelp.review2
    review_cursor = review_collection.find()
    training_file = open("TrainingReviews_2.json", "w")
    line = 0
    training_file.write("[\n")
    for entry in review_cursor:
        if line < size_of_training_review:
            text = entry["text"]
            rating = entry["stars"]
            if text:
                line += 1
                obj = TrainReview(text, rating)
                str_line = json.dumps(vars(obj))
                if line < size_of_training_review:
                    training_file.write("\t" + str_line + ",\n")
                else:
                    training_file.write(str_line)
            if line % 100 == 0:
                print line
        else:
            break
    training_file.write("]")


def gen_review_coll_with_id():
    review2_collection = MongoClient('localhost', 29017).yelp.review2
    review2_cursor = review2_collection.find()
    client = MongoClient('localhost', 29017)
    db = client.yelp
    review_counter = db.review_counter
    counter = 1
    for entry in review2_cursor:
        business_id = entry["business_id"]
        text = entry["text"]
        stars = entry["stars"]
        review_id = entry["review_id"]
        user_id = entry["user_id"]

        _dict = {"business_id": business_id, "text": text, "stars": stars, "review_id": review_id, "user_id": user_id,
                 "counter": counter}
        review_counter.insert(_dict)
        counter += 1

        if counter % 100 == 0:
            print counter

# combine_files()
# get_reviews()
# get_training_reviews(size_of_training_review=4000)

gen_review_coll_with_id()