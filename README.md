# ExerciseSpark2
Solutions of exercises by Jacek Laskowski (https://jaceklaskowski.github.io/spark-workshop/exercises/)  
## Spark SQL

Spark Sql

1. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/split-function-with-variable-delimiter-per-row.html  
    ```
    dept.createOrReplaceTempView("dept")
    spark.sql("select VALUES,Delimiter,SPLIT(if (substr(VALUES,-1)=Delimiter,substr(VALUES, 1, length(VALUES) - 1),VALUES),Delimiter) as split_values from dept").show(false)
    ```
2. --  
  
3. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/adding-count-to-the-source-dataframe.html 
    ```
    val gb = input.groupBy("column1","column2").count()
    input.as("i").join(gb.as("g"),col("i.column1")===col("g.column1")&&col("i.column2")===col("g.column2") ).select(col("i.column0"),col("i.column1"),col("i.column2"),col("g.count")).show
    ```

4. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/limiting-collect_set-standard-function.html  
    ```
    val dummy = input.groupBy("key").agg(collect_set("id").as("all"))
    val take3_ = udf((input: Seq[Long]) => input.take(3))
    dummy.withColumn("only_first_three",take3_(col("all"))).show(false)
    ```
    
5. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/structs-for-column-names-and-values.html  
    ```
    val aa = ratings.select(col("name"),explode(col("movieRatings")).as("expl"))
    val bb = aa.select(col("name"),col("expl.*")).show
    bb.groupBy("name").pivot("movieName").sum("rating").show
    ```

6. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/merge-two-rows.html  
     ```
     input.groupBy(col("id"),col("name")).agg(sum("age").as("age"),max("city").as("city")).show
     ```

7. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/explode-structs-array.html  
    ```
    spark.read.json(spark.sparkContext.wholeTextFiles("/user/test/input.json").values).show
    ```
8. --    
    
9. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-CSV-Data-Source.html  
        cat deniro.csv  
        "Year", "Score", "Title"  
        1968,  86, "Greetings"  
        1970,  17, "Bloody Mama"  
        1970,  73, "Hi, Mom!"  
        1971,  40, "Born to Win"  
        1973,  98, "Mean Streets"  
        1973,  88, "Bang the Drum Slowly"  
        1974,  97, "The Godfather, Part II"  
        1976,  41, "The Last Tycoon"  
        1976,  99, "Taxi Driver"  
        1977,  47, "1900"  
        1977,  67, "New York, New York"  
        
        spark.read.options(Map("inferSchema"->"true", "header"->"true")).csv("/user/test/deniro.csv").show(false)
        
10. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Ids-of-Rows-with-Word-in-Array-Column.html  
    ```
    val aa=input.withColumn("arr",split(col("words"),",")).select(col("id"),col("word"),explode(col("arr")).as("newarr"))
    val bb = aa.groupBy("newarr").agg(collect_set("id"))
    input.as("i").join(bb.as("b"),col("i.word")===col("b.newarr")).select(col("i.word"),col("b.collect_set(id)")).show
    ```

11. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-Dataset-flatMap-Operator.html  
    ```
    nums.flatMap(  r=>r.getSeq[Int](0).map((r.getSeq[Int](0),_) )  ).toDF("nums","num").show
    ```
    
12. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Reverse-engineering-Dataset-show-Output.html  
    ```
    val rddFromFile = spark.sparkContext.textFile("/user/test/input12.txt")
    val aa = rddFromFile.filter(f=> !(f.startsWith("+"))).map(r=>r.split('|'))
    val rowRDD = aa.map(attributes => org.apache.spark.sql.Row(attributes(1), attributes(2), attributes(3)))
    val ss = new StructType().add("id",StringType).add("Text1",StringType).add("Text2",StringType)
    val fi  = spark.createDataFrame(rowRDD,ss)
    ```

13. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Flattening-Array-Columns-From-Datasets-of-Arrays-to-Datasets-of-Array-Elements.html  
    ```
    val l = input.collect()(0).getSeq[String](0).length
    input.select((0 until l).map(i => input("value")(i).alias(s"$i")): _*).show
    ```
14. --  

15. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Most-Populated-Cities-Per-Country.html  
    ```
    val df = spark.read.format("csv").option("header", "true").schema(schema).load("/user/test/input15.csv")
    val df2 = df.withColumn("population",regexp_replace(col("population"), " ", "").cast(IntegerType))
    val df3 = df2.groupBy("country").agg(max("population").as("maxp"))
    df3.as("tab1").join(df2.as("tab2"),col("tab2.country")===col("tab1.country")&&col("tab2.population")===col("tab1.maxp")).select(col("tab2.name"),col("tab2.country"),col("tab1.maxp")).show
    ```

16. [source code](/16/src) 
    ```
    /usr/hdp/2.6.1.0-129/spark2/bin/spark-submit --master yarn-cluster --class "Mainy" testscopty2-assembly-0.1.0-SNAPSHOT.jar --path "/user/test/input16.csv" --col city,country
    ```

17. --  

18. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Difference-in-Days-Between-Dates-As-Strings.html  
My spark 2.1 version, but to_date function with two parameters has been added in 2.2.0
    ```
    dates.createOrReplaceTempView("tmptbl")
    val newdf = spark.sql("select date_string,TO_DATE(CAST(UNIX_TIMESTAMP(date_string, 'MM/dd/yyyy') AS TIMESTAMP))to_date from tmptbl")
    newdf.withColumn("datediff",datediff(current_date(),col("to_date"))).show
    ```
19. --

20. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Why-are-all-fields-null-when-querying-with-schema.html  
    ```
    import org.apache.spark.sql.types.{DateType, StringType, StructType}
    val schema = new StructType().add("dateTime",DateType).add("IP",StringType)
    val df = spark.read.option("delimiter", "|").option("dateFormat", "yyyy-MM-dd HH:mm:ss,SSS").schema(schema).csv("/user/test/input20.csv")
    df.show
    ```

21. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/How-to-add-days-as-values-of-a-column-to-date.html  
    ```
    cat input21.csv
    number_of_days,date
    0,2016-01-1
    1,2016-02-2
    2,2016-03-22
    3,2016-04-25
    4,2016-05-21
    5,2016-06-1
    6,2016-03-21

    val schema = new StructType().add("number_of_days",IntegerType).add("date",DateType)
    val df = spark.read.option("delimiter", ",").option("header", "true").option("dateFormat", "yyyy-MM-dd").schema(schema).csv("/user/test/input21.csv")
    df.withColumn("future", expr("date_add(date, number_of_days)")).show
    ```

22. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-UDFs.html  
    ```
    def mupper(s:String) : String = { s.toUpperCase }
    val mu: (String => String) = mupper
    spark.udf.register("my_upperUDF", mu)
    spark.sql("select my_upperUDF(\"hello\")").show(false)
    ```

23. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-maximum-value-agg.html  
    ```
    val schema = new StructType().add("id",IntegerType).add("name",StringType).add("population",StringType)
    val df = spark.read.option("delimiter", ",").option("header", "true").schema(schema).csv("/user/test/input23.csv")
    val df2 = df.withColumn("population",regexp_replace(col("population"), " ", "").cast(IntegerType))
    df2.select(max(col("population"))).show
    ```

24. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-maximum-values-per-group-groupBy.html  
    ```
    nums.groupBy(col("group")).agg(max(col("id")).as("max_id")).show
    ```

25. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Collect-values-per-group.html  
    ```
    nums.groupBy(col("group")).agg(collect_list(col("id")).as("ids")).show
    ```

26. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Multiple-Aggregations.html  
    ```
    nums.groupBy(col("group")).agg(max(col("id")).as("max_id"), min(col("id")).as("min_id") ).show
    ```

27. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-pivot-to-generate-a-single-row-matrix.html  
    ```
    val df2 = df.groupBy().pivot("udate").agg(first("cc"))
    df2.select(lit("cc").as("update"),$"*").show()
    ```

28. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Using-pivot-for-Cost-Average-and-Collecting-Values.html  
    ```
    data.groupBy("id","type").pivot("date").avg("cost").show
    data.groupBy("id","type").pivot("date").agg(collect_list("ship")).orderBy("id").show
    ```

29. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Pivoting-on-Multiple-Columns.html  
    ```
    data.groupBy("id").pivot("day").agg(first("price").as("price"),first("units").as("unit")).show
    ```

30. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Generating-Exam-Assessment-Report.html  
    ```
    val df = spark.read.option("delimiter", ",").option("header", "true").csv("/user/test/input30.csv")
    val add_qid = udf((input:String) => "Qid_"+input)
    val df2 = df.withColumn("Qid",add_qid(col("Qid")))
    df2.groupBy("ParticipantID","Assessment","GeoTag").pivot("Qid").agg(first("AnswerText")).show
    ```

31. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Flattening-Dataset-from-Long-to-Wide-Format.html  
    ```
    val df = spark.read.option("delimiter", ",").option("header", "true").csv("/user/test/input31.csv")
    df.groupBy("key").pivot("date").agg(first("val1").as("v1"),first("val2").as("v2")).orderBy("key")show
    ```

32. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-1st-and-2nd-Bestsellers-Per-Genre.html  
    ```
    val books = spark.read.option("header", true).option("inferSchema", true).csv("/user/test/input32.csv")
    import org.apache.spark.sql.expressions.Window
    val windowSpec  = Window.partitionBy("genre").orderBy(col("quantity").desc)
    books.withColumn("rank",rank().over(windowSpec)).where(col("rank")<3).show()
    ```
33. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Calculating-Gap-Between-Current-And-Highest-Salaries-Per-Department.html  
    ```
    val df  = spark.read.option("header", true).option("inferSchema", true).csv("/user/test/input33.csv")
    val windowSpecAgg  = Window.partitionBy("department")
    df.withColumn("max", max(col("salary")).over(windowSpecAgg)).withColumn("diff",col("max")-col("salary")).drop("max").show
    ```

34. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Calculating-Running-Total-Cumulative-Sum.html  
    ```
    val windowSpec  = Window.partitionBy("department").orderBy(col("time"))
    df.withColumn("sum", sum(col("items_sold")).over(windowSpec)).show
    ```

35. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Calculating-Difference-Between-Consecutive-Rows-Per-Window.html  
    ```
    val windowSpec  = Window.partitionBy("department").orderBy(col("running_total").asc)
    df.withColumn("lag",-lag("running_total",1,0).over(windowSpec)+col("running_total")).show()
    ```

36. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Converting-Arrays-of-Strings-to-String.html  
    ```
    words.withColumn("solution",concat_ws(" ",col("words"))).show
    ```

37. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Calculating-percent-rank.html  
    ```
    val df  = spark.read.option("header", true).option("inferSchema", true).csv("/user/test/input37.csv") 
    val windowSpec  = Window.orderBy("Salary")
    import org.apache.spark.sql.types.IntegerType
    df.withColumn("Percentage",round(percent_rank().over(windowSpec)*100).cast(IntegerType)).withColumn("Percentage",expr("case when Percentage > 60  then 'Higher' when Percentage > 50 then 'Average' else 'Lower' end")).show
    ```
    
38. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Working-with-Datasets-Using-JDBC-and-PostgreSQL.html  
  
  
    [source code](/38/src)  
    ```
    /usr/hdp/2.6.1.0-129/spark2/bin/spark-submit --master yarn-cluster --class "Main" ex38-assembly-0.1.0-SNAPSHOT.jar \
    --hostname=xxx \
    --port=5432 \
    --database=xxx \
    --username=xxx \
    --password=xxx \
    --table=t1"
    ```

39. --  

40. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-First-Non-Null-Value-per-Group.html  
    ```
    data.groupBy("group").agg(first("id", true)).show
    ```
 
 41. --  
 
 42. https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-Most-Common-Non-null-Prefix-Occurences-per-Group.html  
     ```
     input.groupBy("UNIQUE_GUEST_ID").agg(first("PREFIX")).orderBy("UNIQUE_GUEST_ID").show
     ```
