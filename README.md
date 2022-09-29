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
