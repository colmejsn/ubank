from mysql.connector.utils import print_buffer
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
#Prueba para atomatizar ingreso de tablas
#for q in d:
# if j>0: 
#  d[j]=q+" VARCHAR(255)"
# else:
#   d[j]=q+" INT PRIMARY KEY"
# j+=1

#for q in d:
 #jqr= "ALTER TABLE catalog ADD COLUMN "+q+")"
 #print (jqr)

#conexion a la base de datos
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="mydatabase"
  )
mycursor = mydb.cursor()

#Consulta de transacciones
mycursor.execute("SELECT user_id,transaction_date FROM test_transactions")
myresult = mycursor.fetchall()

#agrupamiento 
df1 = pd.DataFrame(myresult)
tras = df1.groupby([0])
trs1=df1.groupby(0).groups

usuariosnt=[]
usuariosntm=[]
usuariosntt=[]
for name, group in tras:
    if (group[0].count())>600:
      lst1=[]
      lst1.append(name)
      lst1.append(group[1].count())
      usuariosnt.append(lst1)
    if (group[0].count())<20:
      lst2=[]
      lst2.append(name)
      lst2.append(group[1].count())
      usuariosntm.append(lst2)
    #lstt=[]
    #lstt.append(name)
    #lstt.append(group[1].count())
    #usuariosntt.append(lstt)

df2 = pd.DataFrame(usuariosnt)
df2_m = pd.DataFrame(usuariosntm)
#df2_t = pd.DataFrame(usuariosntt)
#Grafico de comparacion
#fig, axs =plt.subplots(1,3,figsize=(9,3), sharey= True)
#axs[0].scatter(df2[0],df2[1])
#axs[1].scatter(df2_m[0],df2_m[1])
#axs[2].scatter(df2_t[0],df2_t[1])
#plt.show()
print(df2)
#df2.to_latex(r'latex2.csv')

projectss=[]
k=0
for id_u in usuariosnt:
  mycursor.execute("SELECT * FROM test_project where user_id = '"+id_u[0]+"'")
  resul = mycursor.fetchall()
  prv=[]
  if (len(resul)<1):    
      projectss.append([0,'null',0,0,0,0])
  for resula in resul:
    for resulb in resula:
      prv.append(resulb)
    projectss.append(prv)
 
df3 = pd.DataFrame(projectss)
print(df3)
#df3.to_latex(r'latex3.csv')

fig, axs = plt.subplots()
axs.scatter(df3[1],df2[1])

plt.show


ruless=[]
tproj=[]
for id_p in projectss:
  if (id_p[0] != 0):
    mycursor.execute("SELECT rule_type_id FROM test_rules where project_id = '"+id_p[0]+"'")
    resul1 = mycursor.fetchall()
    mycursor.execute("SELECT name FROM catalog where id = '"+id_p[4]+"'")
    resul2 = mycursor.fetchall()
    
    apoyo=[]
    for abs in resul1:
      for abs2  in abs:
        mycursor.execute("SELECT name FROM catalog where id = '"+abs2+"'")
        apoyo.append( mycursor.fetchall())
    ruless.append(apoyo)
    tproj.append(resul2)

df5=pd.DataFrame(tproj)
df4=pd.DataFrame(ruless)
#df4.to_latex(r'latex.csv')
print(df5)
#df5.to_latex(r'latex.csv')