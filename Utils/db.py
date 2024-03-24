import json
from pymongo import MongoClient


def regions():
    with open("Data\\data.json", "r") as f:
        data = json.load(f)
    return list(data.keys())


def format_desc_asc(order_by):
    return -1 if order_by == "DESC" else 1


class DatabaseManager:
    def __init__(self, db_name, host='localhost', port=27017):
        self.db_name = db_name
        self.client = MongoClient(host, port)
        self.db = self.client["Student_Advisor"]

    def create_table(self):
        pass

    def add_data(self, table_name, **kwargs):
        data = self.db[table_name]
        last_document = data.find_one(sort=[("_id", -1)])
        last_id = 0

        if last_document:
            last_id = last_document["_id"]
        next_id = last_id + 1
        kwargs["_id"] = next_id
        try:
            kwargs["age"] = str(kwargs["age"])
        except KeyError:
            pass

        self.db[table_name].insert_one(kwargs)

    def get_existing_relations(self, table_name="student_advisor"):
        result = self.db[table_name].find()
        return [(i['student_id'], i['advisor_id'],) for i in result]

    def delete_row(self, table_name, row_id):
        if table_name != "student_advisor":
            self.db[table_name].delete_one({"_id": int(row_id)})
            return
        self.db[table_name].delete_one({"student_id": int(row_id)})

    def load_data(self, table_name):
        data = list(self.db[table_name].find())
        if table_name != "student_advisor":
            return [tuple(item.values()) for item in data]
        return [(i['student_id'], i['advisor_id'],) for i in data]

    def update(self, table_name, name, surname, age, row_id):
        update_data = {"name": name, "surname": surname, "age": age}
        self.db[table_name].update_one({"_id": row_id}, {"$set": update_data})

    def search(self, table_name, **kwargs):
        query = {}
        for key, value in kwargs.items():
            if value != '':
                if table_name != "student_advisor":
                    query[key] = {"$regex": value, "$options": "i"}
                else:
                    query[key] = int(value)

        data = self.db[table_name].find(query)
        if table_name != "student_advisor":
            return [tuple(item.values()) for item in data]
        return [(i['student_id'], i['advisor_id'],) for i in data]

    def list_advisors_with_students_count(self, order_by):
        order_by = format_desc_asc(order_by)
        pipeline = [
            {
                "$lookup": {
                    "from": "student_advisor",
                    "localField": "_id",
                    "foreignField": "advisor_id",
                    "as": "students"
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "name": {"$first": "$name"},
                    "surname": {"$first": "$surname"},
                    "student_count": {"$sum": {"$size": "$students"}}
                }
            },
            {"$sort": {"student_count": order_by}}
        ]

        data = self.db["advisors"].aggregate(pipeline)
        return [tuple(item.values()) for item in data]

    def list_students_with_advisors_count(self, order_by):
        order_by = format_desc_asc(order_by)
        pipeline = [
            {
                "$lookup": {
                    "from": "student_advisor",
                    "localField": "_id",
                    "foreignField": "student_id",
                    "as": "advisors"
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "name": {"$first": "$name"},
                    "surname": {"$first": "$surname"},
                    "advisor_count": {"$sum": {"$size": "$advisors"}}
                }
            },
            {
                "$sort": {"advisor_count": order_by}
            }
        ]

        data = self.db["students"].aggregate(pipeline)
        return [tuple(item.values()) for item in data]

    def check_bd(self):
        for collection_name in self.db.list_collection_names():
            count = self.db[collection_name].count_documents({})
            if count == 0:
                return True
        return False
