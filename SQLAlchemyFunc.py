from sqlalchemy import create_engine, MetaData, literal,select,func,case,and_

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

maxQuery=select(studentTable.c.grade, func.max(studentTable.c.score).label("max score")).group_by(studentTable.c.grade)

minQuery=select(studentTable.c.grade,func.min(studentTable.c.score),func.count()).group_by(studentTable.c.grade)

floorQuery=select(func.floor(3.6))

caseQuery = select(
    studentTable.c.name,studentTable.c.score,
    case(
            (and_(studentTable.c.score>=91,studentTable.c.score<=100),10),
            (and_(studentTable.c.score>=81, studentTable.c.score<=90) , 9),
            (and_(studentTable.c.score>=71, studentTable.c.score<=80) , 8),
            (and_(studentTable.c.score>=61, studentTable.c.score<=70) , 7),
            (and_(studentTable.c.score>=51, studentTable.c.score<=60) , 6),
            (and_(studentTable.c.score>=41, studentTable.c.score<=50) , 5),
            else_=0
    ).label("Grade Points")
)


absQuery=select(func.abs(10),literal(10),func.abs(-20),literal(-20),func.abs(30+43-100),literal(30+43-100))




with engine.connect() as connect:
    
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
    
      
    result=connect.execute(maxQuery).fetchall()
    print("grade || max score")
    #printing the data
    for data in result:
      print(data[0],"||",data[1])
    
    result=connect.execute(minQuery).fetchall()
    print("grade | min value | count")
    #print data
    for data in result:
        print(data[0])
        
    result=connect.execute(caseQuery).fetchall()
    for data in result:
        print(*data)
    




#executing the query with Db
with engine.connect() as connect:
    #retriving tha data
    result=connect.execute(absQuery).fetchall()

print("Abs values of abs value and actual value::",*result[0])
