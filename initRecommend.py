import pandas as pd
from scipy.spatial import distance
import numpy
def recommendBook(data_from_database, averageEvaluation):
    data_item_base_frame = pd.DataFrame(index=data_from_database.index, columns=data_from_database.index)
    # print data_item_base_frame.head(6).ix[:,0:6]
    for i in data_item_base_frame.columns:
        u = data_from_database.loc[i,:];
        u1 = u.index.tolist();
        d1 = [u[index] if u[index] > 0 else averageEvaluation[index] for index in u1];
        for j in data_item_base_frame.columns:
#             print("Dong ",i," Cot ", j)
            v = data_from_database.loc[j,:];
            v1 = v.index.tolist();
            d2 = [v[index] if v[index] > 0 else averageEvaluation[index] for index in v1];
            data_item_base_frame.loc[i, j] = distance.euclidean(d1,d2)
#     data_item_base_frame.to_csv('data_user_base_frame.csv', sep=',')
    # data_item_base_frame = pd.read_csv('data_item_base_frame.csv')
    # print(data_item_base_frame.head(6).iloc[:,0:5])
    # Initial a frame for save closes neighbors to an item
    data_neighbors = pd.DataFrame(index=data_item_base_frame.columns, columns = range(1, 11))
#     # Order by similarity
    for i in range(0, len(data_item_base_frame.columns)):
        print( data_item_base_frame.iloc[0:, i].sort_values(ascending=True)[1:11].index)
        data_neighbors.iloc[i,:10] = data_item_base_frame.iloc[0:, i].sort_values(ascending=True)[1:11].index 
        
    data_sims = pd.DataFrame(index=data_neighbors.index,columns=range(1, 11))
    for i in range(0, len(data_item_base_frame.index)):
        list2 = data_from_database.iloc[i,:].tolist();
        data_tmp = pd.DataFrame(index=data_neighbors.iloc[i,:], columns = data_from_database.columns)
        for k in data_tmp.index:
            list1 = data_from_database.loc[k,:];
            list_tmp = list1.index.tolist();
            listCompare = [list1[ind] if list1[ind] > 0 else averageEvaluation[ind] for ind in list_tmp]
            data_tmp.loc[k,:] = numpy.subtract(listCompare,list2);
            
        data_sims.iloc[i,:] = data_tmp.max().nlargest(10).index
    return data_sims

def recommendBookForUser(data_from_database,id_user,recommend_data, averageEvaluation):
    data_item_base_frame = pd.DataFrame(index=[id_user], columns=data_from_database.index)
    u = data_from_database.loc[id_user,:];
    u1 = u.index.tolist();
    d1 = [u[index] if u[index] > 0 else averageEvaluation[index] for index in u1];
    for j in data_item_base_frame.columns:
        v = data_from_database.loc[j,:];
        v1 = v.index.tolist();
        d2 = [v[index] if v[index] > 0 else averageEvaluation[index] for index in v1];
        data_item_base_frame.loc[id_user, j] = distance.euclidean(d1, d2)
        
    data_neighbors = pd.DataFrame(index=[id_user], columns = range(1, 11))
    data_neighbors.loc[id_user,:10] = data_item_base_frame.loc[id_user,:].sort_values(ascending=True)[1:11].index
    data_tmp = pd.DataFrame(index=data_neighbors.loc[id_user,:], columns = data_from_database.columns)
    list2 = data_from_database.loc[id_user,:].tolist();
    for k in data_tmp.index:
        list1 = data_from_database.loc[k,:];
        list_tmp = list1.index.tolist();
        listCompare = [list1[ind] if list1[ind] > 0 else averageEvaluation[ind] for ind in list_tmp]
        data_tmp.loc[k,:] = numpy.subtract(listCompare,list2);
    
            
    recommend_data.loc[id_user,:] = data_tmp.max().nlargest(10).index.tolist()
    return;
    
def reRecommendBookForUser(data_from_database,recommend_data, averageValue):
    recommend_data = recommendBook(data_from_database, averageValue);
    return