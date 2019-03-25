from copy import deepcopy
import csv
from flask import Flask, render_template, request,flash
import pypyodbc
import random
import numpy as np
import scipy
import math


from time import time
import itertools
from sklearn.cluster import KMeans



app = Flask(__name__)
app.secret_key = "Secret"

connection = pypyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=(specify server name);Database=(specify databse name);Uid=(specify user id);Pwd=(specify pwd);")
cursor = connection.cursor()

@app.route('/')
def index():
   return render_template('index.html')

@app.route('/search', methods=['POST', 'GET'])
def search():
     start_time = time()
     k = request.args.get("k")#no. of clusters


     cursor.execute("Select * from minnow ")
     rows=cursor.fetchall() #fetches value
     # pclass = []
     # boat = []
     # survival = []
     Age = []
     Fare = []

     for row in rows:
         # pclass.append(row[0])
         # survival.append(row[1])
         # if row[11] == '':
         #     boat.append(0)
         # else:
         #     boat.append(row[11])
         if row[3] == '' or row[4]== None:
             Age.append(0)
         else:
             Age.append(row[3])
         if row[9] == '' or row[9]== None:
             Fare.append(0)
         else:
             Fare.append(row[9])

     connection.close()


     X = np.array(list(zip(Age[:len(Age) - 1], Fare[:len(Fare) - 1])))



     km = KMeans(n_clusters=int(k))
     km.fit(X)
     centroids = km.cluster_centers_
     labels = km.labels_
     dhruvi={i: X[np.where(km.labels_ == i)] for i in range(km.n_clusters)} #one cluster at a time eg: 0:[...,..[,[...]

     label_length=len(dhruvi) #same as no of clusters

     length_value=[] #cpunt all valuesof single single labels
     # eg:len_values = [[32],[10],[4],[70]]    ...i.e lengths of 4 labels
     for i in range(len(dhruvi)):
         length_value.append(len(dhruvi[i]))



     colors = ["g.", "r.", "b.", "y." "c.", "m.", "k.", "w."]

     # for i in range(len(X)):
     #     plt.plot(X[i][0], X[i][1], colors[labels[i]], markersize = 10)
     # plt.scatter(centroids[:,0],centroids[:,1],marker = "x", s = 150, linewidths=5, zorder = 10)
     displaylist = list(zip(Age,Fare,labels))
     print('\n\nDisplay List------------------------------------', displaylist)
     dist_list = [] #for calculating distance
     for i in range(0, len(centroids) - 1):
         for j in range(i + 1, len(centroids)):

             x1 = centroids[i][0]
             x2 = centroids[j][0]
             y1 = centroids[i][1]
             y2 = centroids[j][1]
             temp = (x1 - x2)*(x1 - x2) + (y1 - y2)*(y1 - y2)
             dist = math.sqrt(temp)

             dist_list.append(list(zip(centroids[i][:], centroids[j][:], itertools.repeat(dist))))

     print(dist_list)
     dist_len = len(dist_list)


     end_time = time()
     time_taken = (end_time - start_time)
     flash('The Average Time taken to execute the random queries is : ' + "%.4f" % time_taken + " seconds")

     return render_template('output.html', t=time_taken,my=displaylist, centroid=centroids, distances = dist_list, length=dist_len,dhruvi=dhruvi,length_value=length_value,label_length=label_length)

@app.route('/insert')
def insert():

    cursor.execute("CREATE TABLE [dbo].[minnow](\
    	[CabinNum][int] NULL,\
    	[Fname] [nvarchar] (50) NULL,\
    	[Lname][nvarchar] (50) NULL,\
    	[Age] [int] NULL,\
        [Survived] [nvarchar](50) NULL,\
    	[Lat] [int] NULL,\
    	[Long] [int] NULL,\
        [PictureCap][nvarchar](50) NULL,\
        [PicturePas][nvarchar](50) NULL,\
        [Fare][int] NULL)")
    connection.commit()




    query = "INSERT INTO dbo.minnow (CabinNum,Fname,Lname,Age,Survived,Lat,Long,PictureCap,PicturePas,Fare) VALUES (?,?,?,?,?,?,?,?,?,?)"


    with open('minnow.csv') as csvfile:
          next(csvfile)
          reader = csv.reader(csvfile, delimiter=',')
          for row in reader:
              print(row)
              cursor.execute(query,row)

          connection.commit()

    return render_template('insert.html')
# ************************************************************
@app.route('/select', methods = ['POST', 'GET'])
def select():
    cursor = connection.cursor()
    cabin = request.args.get("cabin")
    cursor.execute("select * from minnow where CabinNum='" + str(cabin) + "'")
    o_rows=cursor.fetchall()
    return render_template('select.html',r=o_rows)



@app.route('/count',methods = ['POST', 'GET'])
def count():

    mag = request.args.get("mag")
    cursor.execute("Select count(*) from edata where mag>='"+mag+"'")
    rows=cursor.fetchall()

    return render_template('count.html', a=rows)
@app.route('/look', methods = ['POST', 'GET'])
def look():

    k = request.args.get("k")
    min = request.args.get("min")
    max = request.args.get("max")

    start_time = time()
    for i in range(0, int(k)):
        mag = random.uniform(float(min), float(max))
        cursor.execute("select  top 1 locationSource from earthquakes where mag>='"+str(mag)+"'")
        result=cursor.fetchall()

    end_time = time()
    time_taken = (end_time - start_time) / int(k)
    flash('The Average Time taken to execute the random queries is : ' + "%.4f" % time_taken + " seconds")
    return render_template('random.html',t=time_taken,r=result)


@app.route('/list',methods = ['POST', 'GET'])
def list():

    min = request.args.get("min")
    max = request.args.get("max")
    # loc = request.args.get("loc")
    cursor.execute("select  latitude, longitude, place from edata where mag Between '"+min+"' and '"+max+"' ")
    rows=cursor.fetchall()
    print(rows)

    return render_template('list.html', ci=rows)



if __name__ == '__main__':
   app.run(debug = True)
