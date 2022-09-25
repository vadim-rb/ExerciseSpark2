# ExerciseSpark2
Solutions of exercises by Jacek Laskowski (https://jaceklaskowski.github.io/spark-workshop/exercises/)  
## Spark SQL

Spark Sql

1. https://jaceklaskowski.github.io/spark-workshop/exercises/sql/split-function-with-variable-delimiter-per-row.html  
    ```
    dept.createOrReplaceTempView("dept")
    spark.sql("select VALUES,Delimiter,SPLIT(if (substr(VALUES,-1)=Delimiter,substr(VALUES, 1, length(VALUES) - 1),VALUES),Delimiter) as split_values from dept").show(false)
    ```
  
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
