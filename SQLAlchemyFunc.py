from sqlalchemy import create_engine, MetaData,select,func

# Create an engine and metadata
engine = create_engine("mysql+pymysql://root:Ravi123@localhost:3306/study")
metadata = MetaData()
metadata.reflect(bind=engine)

#Getting the refference of employee table from DB
studentTable=metadata.tables['student']

#Query to find the average of student's score
averageQuery=select(func.avg(studentTable.c.score).label("Average"))

countQuery=select(func.count(studentTable.c.name).label("Count of Students"))

sumQuery=select(func.sum(studentTable.c.score).label("Sum of scores"))

lenQuery=select(studentTable.c.name,func.length(studentTable.c.name).label("Sum of scores"))

upperQuery=select(func.upper(studentTable.c.name).label("Upper"))

nowQuery=select(func.now().label("now"))

groupConcatQuery = select(
    studentTable.c.grade,
    func.group_concat(studentTable.c.name, ',').label('names')
).group_by(studentTable.c.grade)



#executing the query with Db
with engine.connect() as connect:
    #retriving tha data
    avgResult=connect.execute(averageQuery).fetchall()
    print("Average of Student's score is::",avgResult[0][0],"\n")

    
    cntResult=connect.execute(countQuery).fetchall()
    print("Total count of students in class::",cntResult[0][0],"\n")
    
    sumResult=connect.execute(sumQuery).fetchall()
    print("Sum of scores of students::",sumResult[0][0],"\n")    

    
    lenResult=connect.execute(lenQuery).fetchall()
    print("Student name and length of name")
    for data in lenResult:
        print(data[0],data[1])
    print()
        
    upperResult=connect.execute(upperQuery).fetchall()
    print("Student Names in UPPER Case")
    #printing the data
    for data in upperResult:
        print(data[0])
    print()
    nowResult=connect.execute(nowQuery).fetchall()
    #printing the data
    print("now() result::",nowResult[0][0],"\n")
    
    groupConcatResult=connect.execute(groupConcatQuery).fetchall()
    print("group concat query result")
    for data in groupConcatResult:
        print(data[0],data[1])