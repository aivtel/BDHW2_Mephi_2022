from random import randrange
from datetime import timedelta
from datetime import datetime
import random
import re
import json

def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

date_format = '%Y-%m-%d %H:%M:%S'
start_date = "2022-03-01 00:00:00"
end_date = "2022-06-25 00:00:00"

start_datetime = datetime.strptime(start_date, date_format)
end_datetime = datetime.strptime(end_date, date_format)

groups = []
group_dict = []

messages = ["Hello!", "Bye-bye", "Wow", "Good for you", "Awesome"]

for i in range(30):
    groups.append("group_" + str(i))

for i in range(1000):
    random_group = random.choice(groups)
    username = "User_" + str(i)
    new_record = dict()
    new_record.update({'user': username, 'group': random_group})
    group_dict.append(new_record)

def filter_users(dicts_of_users, search_group, username_to_avoid):
    def iterator_func(x):
        group = x.get("group")
        user = x.get("user")
        if group == search_group and user != username_to_avoid:
                return True
        return False
    return filter(iterator_func, dicts_of_users)

f = open("messages.txt", 'wb')
for i in range(15000):
   random_user = random.choice(group_dict)
   user1 = random_user.get("user")
   group = random_user.get("group")
   users_with_same_group = filter_users(group_dict, group, user1)
   random_user_2 = random.choice(users_with_same_group)
   user2 = random_user_2.get("user")
   random_message = random.choice(messages)
   random_timestamp = str(random_date(start_datetime, end_datetime))
   result_str = user1 + ";" + user2 + ";" + random_timestamp + ";" + random_message + "\n"
   f.write(str.encode(result_str))

f.close()


jsonString = json.dumps(group_dict)
jsonFile = open("dict.json", "wb")
jsonFile.write(jsonString)
jsonFile.close()
