from flask import Flask, request
from flask_restful import Resource, Api
# from flask.json import jsonify
import pandas as pd
from getDataFromDB import getListData, getListObject
import numpy
from initRecommend import recommendBook, recommendBookForUser, reRecommendBookForUser
import _thread


passcode = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJMb2dpbiIsImRhdGEiOnsiaWQiOjEyLCJ1c2VybmFtZSI6ImNhb2RhbzEyMzQzNCJ9LCJpYXQiOjE1NDQxODk5MjMsImV4cCI6MTU0NTA1MzkyM30.P4_BTd-ZuOtvQd0QlazlKigmDl9sVnvoKgjWjXmspdA";
allUser = getListData("select id from users where id != 0 and role_id = 3");
allItem = getListData("select bid from book where is_active = true");
data_from_database = pd.DataFrame(index=allUser,columns=allItem)
data_from_database[:] = 0;

allBying = getListObject("SELECT id_user, id_pro FROM orders INNER JOIN detailorder ON orders.id_order = detailorder.id_order WHERE id_user != 0 AND orders.id_status != 2 GROUP BY id_user, id_pro")  
for i in allBying:
    if(i[0] in data_from_database.index and i[1] in data_from_database.columns):
        data_from_database.loc[i[0],i[1]] = 1;
        
# print(allBying)        
# print(data_from_database.loc[:27,43:])
dictAverageScore = {};

for i in data_from_database.columns:
    listData = [item for item in data_from_database.loc[:,i] if item >= 0]
    if len(listData) == 0:
        dictAverageScore[i] = 0;
    else:
        dictAverageScore[i] = numpy.sum(listData) / len(data_from_database.index);
     
recommend_data = recommendBook(data_from_database, dictAverageScore) 
# print(data_from_database.loc[:27,43:])
print(recommend_data)
app = Flask(__name__)
api = Api(app)

class RecommendBook(Resource):
    def get(self, id_user):
        id_user = numpy.int64(id_user)
        passcodeFromClient = request.headers.get('passcode');
        if passcodeFromClient != passcode:
            return

        result = recommend_data.loc[id_user,:].tolist();
        
        print(result)
        return {"data" : [x for x in result if x != -1]  }
    
class AddNewBook(Resource):
    def post(self):
        passcodeFromClient = request.headers.get('passcode')
        if passcodeFromClient != passcode:
            return
        id_book = request.form['id_book']
        id_book = numpy.int64(id_book)
        global dictAverageScore
        dictAverageScore[id_book] = 0;
        global data_from_database
        global recommend_data
        data_from_database.insert(loc=0, column=id_book, value= [0] * len(data_from_database.index))
#         data_from_database = data_from_database.assign(id_location=[-5] * len(data_from_database.index))
#         data_from_database.rename(columns={'id_location': id_location}, inplace=True)
#         data_from_database.loc[:,id_location] = [-5] * len(data_from_database.index)
        print("AddNewBook ok")
        return {"data" : "OK"}

class AddNewUser(Resource):
    def post(self):
        passcodeFromClient = request.headers.get('passcode')
        if passcodeFromClient != passcode:
            return
        id_user = request.form['id_user']
        id_user = numpy.int64(id_user)
        
        global dictAverageScore
        global data_from_database
        global recommend_data
#         columns_data = data_from_database.columns;
#         data_add = [-5] * len(columns_data);
        data_from_database = data_from_database.append(pd.Series([0] * len(data_from_database.columns), index=data_from_database.columns, name=id_user))
        
#         data_from_database = data_from_database.append(pd.DataFrame(data_add,index=id_user,columns=columns_data))
#         data_from_database.loc[id_user,:] = [-5] * len(data_from_database.columns);
#         recommend_data.loc[id_user,:] = data_from_database.columns.sort_values(ascending=False)[0:10]
        recommend_data = recommend_data.append(pd.Series(data_from_database.columns.sort_values(ascending=False)[0:10], index=recommend_data.columns, name=id_user))
        recommendBookForUser(data_from_database, id_user, recommend_data, dictAverageScore)
        print("AddNewUser ok")
        return {"data" : "OK"}

class BuyBook(Resource):
    def post(self):
        passcodeFromClient = request.headers.get('passcode')
        if passcodeFromClient != passcode:
            return
        list_id_book = [int(x) for x in request.form['list_id_book'].split()] 
        
        id_user = request.form['id_user']
        id_user = numpy.int64(id_user)
        
        global dictAverageScore
        global data_from_database
        global recommend_data
        
        is_change = False
        for id_book in list_id_book:
            if id_book in data_from_database.columns:
                listData = [item for item in data_from_database.loc[:,id_book].tolist() if item >= 0];
                if len(listData) == 0:
                    dictAverageScore[id_book] = 0;
                else:
                    dictAverageScore[id_book] = numpy.sum(listData) / len(data_from_database.index);
                
                if data_from_database.loc[id_user,id_book] != 1:
                    data_from_database.loc[id_user,id_book] = 1;
                    is_change = True
        
        if is_change == True:     
            _thread.start_new_thread(recommendBookForUser, (data_from_database, id_user, recommend_data, dictAverageScore) )        
        print("BUY BOOK ok")
        return {"data" : "OK"}

class CancelBuyBook(Resource):
    def post(self):
        passcodeFromClient = request.headers.get('passcode')
        if passcodeFromClient != passcode:
            return
        list_id_book = [int(x) for x in request.form['list_id_book'].split()] 
        
        id_user = request.form['id_user']
        id_user = numpy.int64(id_user)
        id_order = request.form['id_order']
        id_order = numpy.int64(id_order)
        
        global dictAverageScore
        global data_from_database
        global recommend_data
        
        is_change = False
        for id_book in list_id_book:
            if id_book in data_from_database.columns:
                strSQl = 'select id_detailOrder from detailorder INNER JOIN orders ON detailorder.id_order = orders.id_order where id_status != 2 and id_pro = ' + str(id_book) + ' and detailorder.id_order != ' + str(id_order) + ' and id_user = ' + str(id_user);
                list_id_detailOrder = getListData(strSQl);
                if not(list_id_detailOrder) or len(list_id_detailOrder) == 0 :
                    data_from_database.loc[id_user,id_book] = 0; 
                    listData = [item for item in data_from_database.loc[:,id_book].tolist() if item >= 0];
                    if len(listData) == 0:
                        dictAverageScore[id_book] = 0;
                    else:
                        dictAverageScore[id_book] = numpy.sum(listData) / len(data_from_database.index);
                    is_change = True
                        
        
        if is_change == True:     
            _thread.start_new_thread(recommendBookForUser, (data_from_database, id_user, recommend_data, dictAverageScore) )        
        print("CANCEL BUY BOOK ok")
        return {"data" : "OK"}

class ActiveBook(Resource):
    def post(self):
        passcodeFromClient = request.headers.get('passcode')
        if passcodeFromClient != passcode:
            return
        
        id_book = request.form['id_book']
        id_book = numpy.int64(id_book)
        
        global dictAverageScore
        global data_from_database
        global recommend_data
        
        
        if id_book not in data_from_database.columns:
            data_from_database.insert(loc=0, column=id_book, value= [0] * len(data_from_database.index))
            strSQl = 'select id_user from detailorder INNER JOIN orders ON detailorder.id_order = orders.id_order where id_status != 2 and id_pro = ' + str(id_book);
            list_id_user = getListData(strSQl);
            if list_id_user and len(list_id_user) > 0 :
                dictAverageScore[id_book] = len(list_id_user) / len(data_from_database.index);
                for id_user in list_id_user:
                    data_from_database.loc[id_user,id_book] = 1;
#                     _thread.start_new_thread(recommendBookForUser, (data_from_database, id_user, recommend_data, dictAverageScore) )
            else:
                dictAverageScore[id_book] = 0;       
        _thread.start_new_thread(reRecommendBookForUser,(data_from_database,recommend_data, dictAverageScore ))                 
        print("ACTIVE BOOK ok")
        return {"data" : "OK"}

class DeleteUser(Resource):
    def post(self):
        passcodeFromClient = request.headers.get('passcode')
        if passcodeFromClient != passcode:
            return
        id_user = request.form['id_user']
        id_user = numpy.int64(id_user)
        global dictAverageScore
        global data_from_database
        global recommend_data
        data_from_database.drop(id_user,inplace=True)
        recommend_data.drop(id_user,inplace=True)
        print("DELETE USER ok")
        return {"data" : "OK"}
    
class DeleteBook(Resource):
    def post(self):
        passcodeFromClient = request.headers.get('passcode')
        if passcodeFromClient != passcode:
            return
        id_book = request.form['id_book']
        id_book = numpy.int64(id_book)
        
        global dictAverageScore
        global data_from_database
        global recommend_data
        
        del dictAverageScore[id_book]
        del data_from_database[id_book]
        #recommend_data.replace(id_book,-1,inplace=True)
        list_id_user = [id_user for id_user in data_from_database.index if id_book in recommend_data.loc[id_user, :].tolist() ]
        for id_user in list_id_user:
            _thread.start_new_thread(recommendBookForUser, (data_from_database, id_user, recommend_data, dictAverageScore) )
        print("DeleteBook ok")
        return {"data" : "OK" } 
#     
api.add_resource(RecommendBook, '/recommendBook/<id_user>')
api.add_resource(AddNewBook, '/addBook')
api.add_resource(AddNewUser, '/addUser')
api.add_resource(BuyBook, '/buyBook')
api.add_resource(ActiveBook, '/activeBook')
api.add_resource(CancelBuyBook, '/cancelBuyBook')
api.add_resource(DeleteUser, '/deleteUser')
api.add_resource(DeleteBook, '/deleteBook')
    
if __name__ == '__main__':
    app.run(port='5002')
    