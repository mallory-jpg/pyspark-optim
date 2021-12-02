# PySpark Optimization 

After looking at the query plan for the original execution, I chose to:
* **Cache** the answers table for use in the `resultDF`
* **Reorder join** between `answers_by_month` and `questionsDF`
* **Broadcast** the `questionsDF` to hasten its join to the `answers_month` table 

Submit Spark job: `./bin/spark-submit --master spark://207.184.161.138:7077 optimize.py`

### Improving Queries
There are many options we can use to optimize PySpark code.
1. Make the most of Spark Operators
2. Decrease the number & size of shuffles
3. Tuning Resource Allocation
4. Tuning the # of partitions
5. Reducing the size of data structures
6. Choosing proper data formats 

## Data
### Answers DF
**`answersDF` Schema**
```
root
 |-- question_id: long (nullable = true)
 |-- tags: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- creation_date: timestamp (nullable = true)
 |-- title: string (nullable = true)
 |-- accepted_answer_id: long (nullable = true)
 |-- comments: long (nullable = true)
 |-- user_id: long (nullable = true)
 |-- views: long (nullable = true)
 ```
 **`answersDF` Sample**
 ```
 +-----------+---------+--------------------+--------+-------+-----+
|question_id|answer_id|       creation_date|comments|user_id|score|
+-----------+---------+--------------------+--------+-------+-----+
|     226592|   226595|2015-12-29 17:46:...|       3|  82798|    2|
|     388057|   388062|2018-02-22 12:52:...|       8|    520|   21|
|     293286|   293305|2016-11-17 15:35:...|       0|  47472|    2|
|     442499|   442503|2018-11-22 00:34:...|       0| 137289|    0|
|     293009|   293031|2016-11-16 07:36:...|       0|  83721|    0|
|     395532|   395537|2018-03-25 00:51:...|       0|   1325|    0|
|     329826|   329843|2017-04-29 10:42:...|       4|    520|    1|
|     294710|   295061|2016-11-26 19:29:...|       2| 114696|    2|
|     291910|   291917|2016-11-10 04:56:...|       0| 114696|    2|
|     372382|   372394|2017-12-03 20:17:...|       0| 172328|    0|
|     178387|   178394|2015-04-25 12:31:...|       6|  62726|    0|
|     393947|   393948|2018-03-17 17:22:...|       0| 165299|    9|
|     432001|   432696|2018-10-05 03:47:...|       1| 102218|    0|
|     322740|   322746|2017-03-31 13:10:...|       0|    392|    0|
|     397003|   397008|2018-04-01 06:31:...|       1| 189394|    6|
|     223572|   223628|2015-12-11 23:40:...|       0|  94772|   -1|
|     220328|   220331|2015-11-24 09:57:...|       3|  92883|    1|
|     176400|   176491|2015-04-16 08:13:...|       0|  40330|    0|
|     265167|   265179|2016-06-28 06:58:...|       0|  46790|    0|
|     309103|   309105|2017-02-01 11:00:...|       2|  89597|    2|
+-----------+---------+--------------------+--------+-------+-----+
```
### Questions DF
**`questionsDF` Schema**
```
root
 |-- question_id: long (nullable = true)
 |-- answer_id: long (nullable = true)
 |-- creation_date: timestamp (nullable = true)
 |-- comments: long (nullable = true)
 |-- user_id: long (nullable = true)
 |-- score: long (nullable = true) 
 ```
 **`questionsDF` Sample**
 ```
 +-----------+--------------------+--------------------+--------------------+------------------+--------+-------+-----+
|question_id|                tags|       creation_date|               title|accepted_answer_id|comments|user_id|views|
+-----------+--------------------+--------------------+--------------------+------------------+--------+-------+-----+
|     382738|[optics, waves, f...|2018-01-28 01:22:...|What is the pseud...|            382772|       0|  76347|   32|
|     370717|[field-theory, de...|2017-11-25 03:09:...|What is the defin...|              null|       1|  75085|   82|
|     339944|[general-relativi...|2017-06-17 15:32:...|Could gravitation...|              null|      13| 116137|  333|
|     233852|[homework-and-exe...|2016-02-04 15:19:...|When does travell...|              null|       9|  95831|  185|
|     294165|[quantum-mechanic...|2016-11-22 05:39:...|Time-dependent qu...|              null|       1| 118807|   56|
|     173819|[homework-and-exe...|2015-04-02 10:56:...|Finding Magnetic ...|              null|       5|  76767| 3709|
|     265198|    [thermodynamics]|2016-06-28 09:56:...|Physical meaning ...|              null|       2|  65035| 1211|
|     175015|[quantum-mechanic...|2015-04-08 20:24:...|Understanding a m...|              null|       1|  76155|  326|
|     413973|[quantum-mechanic...|2018-06-27 08:29:...|Incorporate spino...|              null|       3| 167682|   81|
|     303670|[quantum-field-th...|2017-01-08 00:05:...|A Wilson line pro...|              null|       0| 101968|  184|
|     317368|[general-relativi...|2017-03-08 13:53:...|Shouldn't Torsion...|              null|       0|  20427|  305|
|     369982|[quantum-mechanic...|2017-11-20 21:11:...|Incompressible in...|              null|       4| 124864|   83|
|     239745|[quantum-mechanic...|2016-02-25 02:51:...|Is this correct? ...|            239773|       2|  89821|   78|
|     412294|[quantum-mechanic...|2018-06-17 19:46:...|Is electron/photo...|              null|       0|    605|   61|
|     437521|[thermodynamics, ...|2018-10-29 01:49:...|Distance Dependen...|              null|       2| 211152|   19|
|     289701|[quantum-field-th...|2016-10-29 22:56:...|Generalize QFT wi...|              null|       4|  31922|   49|
|     239505|[definition, stab...|2016-02-24 04:51:...|conditions for so...|              null|       3| 102021|  121|
|     300744|[electromagnetism...|2016-12-24 12:14:...|Maxwell equations...|            300749|       0| 112190|  171|
|     217315|[nuclear-physics,...|2015-11-08 03:13:...|Is the direction ...|              null|       1|  60150| 1749|
|     334778|[cosmology, cosmo...|2017-05-22 08:58:...|Why are fluctatio...|            334791|       3| 109312|  110|
+-----------+--------------------+--------------------+--------------------+------------------+--------+-------+-----+
```

## Query 1: Aggregation of Questions Table
Goal: Count the number of questions grouped by `question_id` and `month`.

**Result**
```
+-----------+-----+---+
|question_id|month|cnt|
+-----------+-----+---+
|     358894|    9|  5|
|     332782|    5|  2|
|     281552|    9|  2|
|     332224|    5|  1|
|     395851|    3|  3|
|     192346|    7|  1|
|     302487|    1|  3|
|     317571|    3|  2|
|     179458|    5|  2|
|     294966|   11|  5|
|     199602|    8|  6|
|     251275|    4|  2|
|     208722|    9|  1|
|     284125|   10|  1|
|     427452|    9|  4|
|     399738|    4|  1|
|     217997|   11|  4|
|     386225|    2|  1|
|     305095|    1|  3|
|     206822|    9|  6|
+-----------+-----+---+
only showing top 20 rows
```

### Unoptimized Query
```
answers_month = answersDF.withColumn('month', month('creation_date')) \
                         .groupBy('question_id', 'month') \
                         .agg(count('*') \
                         .alias('cnt') 
```

Query Plan
```
== Physical Plan ==
*(2) HashAggregate(keys=[question_id#0L, month#110], functions=[count(1)])
+- Exchange hashpartitioning(question_id#0L, month#110, 200), true, [id=#100]
   +- *(1) HashAggregate(keys=[question_id#0L, month#110], functions=[partial_count(1)])
      +- *(1) Project [question_id#0L, month(cast(creation_date#2 as date)) AS month#110]
         +- *(1) ColumnarToRow
            +- FileScan parquet [question_id#0L,creation_date#2] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/Users/mallory/Desktop/DataEngineering/Springboard/DistributedComputing/Sp..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<question_id:bigint,creation_date:timestamp>

```
### Optimized Query
```
answers_by_month = answersDF.withColumn('month', month('creation_date')) \
                            .groupBy('question_id', 'month') \
                            .agg(count('*') \
                            .alias('cnt')) \

# cache
answers_by_month.cache()
```
* **Caching** improved performance greatly, likely due to the size of the dataset and because the master and worker nodes were on the same machine.

Optimized Query Plan
```
== Physical Plan ==
*(1) ColumnarToRow
+- InMemoryTableScan [question_id#347L, month#375, cnt#391L]
      +- InMemoryRelation [question_id#347L, month#375, cnt#391L], StorageLevel(disk, memory, deserialized, 1 replicas)
            +- *(2) HashAggregate(keys=[question_id#347L, month#375], functions=[count(1)])
               +- Exchange hashpartitioning(question_id#347L, month#375, 200), ENSURE_REQUIREMENTS, [id=#206]
                  +- *(1) HashAggregate(keys=[question_id#347L, month#375], functions=[partial_count(1)])
                     +- *(1) Project [question_id#347L, month(cast(creation_date#349 as date)) AS month#375]
                        +- *(1) ColumnarToRow
                           +- FileScan parquet [question_id#347L,creation_date#349] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/Users/mallory/Desktop/DataEngineering/Springboard/DistributedComputing/Sp..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
```


## Query 2: Joining Question & Answer Tables
Goal: Join the `answers_month` table to the `questionsDF` on the `question_id` column.

**Result**
```
+-----------+--------------------+--------------------+-----+---+
|question_id|       creation_date|               title|month|cnt|
+-----------+--------------------+--------------------+-----+---+
|     155989|2014-12-31 19:59:...|Frost bubble form...|    2|  1|
|     155989|2014-12-31 19:59:...|Frost bubble form...|   12|  1|
|     155990|2014-12-31 20:51:...|The abstract spac...|    1|  2|
|     155992|2014-12-31 21:44:...|centrifugal force...|    1|  1|
|     155993|2014-12-31 21:56:...|How can I estimat...|    1|  1|
|     155995|2014-12-31 23:16:...|Why should a solu...|    1|  3|
|     155996|2015-01-01 00:06:...|Why do we assume ...|    1|  2|
|     155996|2015-01-01 00:06:...|Why do we assume ...|    2|  1|
|     155996|2015-01-01 00:06:...|Why do we assume ...|   11|  1|
|     155997|2015-01-01 00:26:...|Why do square sha...|    1|  3|
|     155999|2015-01-01 01:01:...|Diagonalizability...|    1|  1|
|     156008|2015-01-01 02:48:...|Capturing a light...|    1|  2|
|     156008|2015-01-01 02:48:...|Capturing a light...|   11|  1|
|     156016|2015-01-01 04:31:...|The interference ...|    1|  1|
|     156020|2015-01-01 05:19:...|What is going on ...|    1|  1|
|     156021|2015-01-01 05:21:...|How to calculate ...|    2|  1|
|     156022|2015-01-01 05:55:...|Advice on Major S...|    1|  1|
|     156025|2015-01-01 06:32:...|Deriving the Cano...|    1|  1|
|     156026|2015-01-01 06:49:...|Does Bell's inequ...|    1|  3|
|     156027|2015-01-01 06:49:...|Deriving X atom f...|    1|  1|
+-----------+--------------------+--------------------+-----+---+
only showing top 20 rows
```
### Unoptimized Query
```
resultDF = questionsDF.join(answers_month, 'question_id') \
                      .select('question_id', 'creation_date', 'title', 'month', 'cnt')
```

Unoptimized Query Plan
```
== Physical Plan ==
*(3) Project [question_id#12L, creation_date#14, title#15, month#110, cnt#126L]
+- *(3) BroadcastHashJoin [question_id#12L], [question_id#0L], Inner, BuildRight
   :- *(3) Project [question_id#12L, creation_date#14, title#15]
   :  +- *(3) Filter isnotnull(question_id#12L)
   :     +- *(3) ColumnarToRow
   :        +- FileScan parquet [question_id#12L,creation_date#14,title#15] Batched: true, DataFilters: [isnotnull(question_id#12L)], Format: Parquet, Location: InMemoryFileIndex[file:/home/dtn/notebooks/data/questions], PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#200]
      +- *(2) HashAggregate(keys=[question_id#0L, month#110], functions=[count(1)])
         +- Exchange hashpartitioning(question_id#0L, month#110, 200), true, [id=#196]
            +- *(1) HashAggregate(keys=[question_id#0L, month#110], functions=[partial_count(1)])
               +- *(1) Project [question_id#0L, month(cast(creation_date#2 as date)) AS month#110]
                  +- *(1) Filter isnotnull(question_id#0L)
                     +- *(1) ColumnarToRow
                        +- FileScan parquet [question_id#0L,creation_date#2] Batched: true, DataFilters: [isnotnull(question_id#0L)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/mallory/Desktop/DataEngineering/Springboard/DistributedComputing/Sp..., PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp>

```

### Optimized Query
`resultDF = answers_by_month.join(broadcast(questionsDF), 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')`

* **Changed join order** because len(`questionsDF`) > len(`answers_by_month`) by a lot. For inner joins, joining the larger table to a smaller table without duplicates (instead of the other way around, small join to large) reduces the number of rows that get passed to the rest of the query.
* **Broadcast join** is best when one dataset is small enough to be store in-memory. This supplies a copy of the broadcasted data to each executor node so that each is self-sufficient in joining to the bigger dataset.

Optimized Query Plan
```
== Physical Plan ==
*(2) Project [question_id#347L, creation_date#361, title#362, month#375, cnt#391L]
+- *(2) BroadcastHashJoin [question_id#347L], [question_id#359L], Inner, BuildRight, false
   :- *(2) Filter isnotnull(question_id#347L)
   :  +- *(2) ColumnarToRow
   :     +- InMemoryTableScan [question_id#347L, month#375, cnt#391L], [isnotnull(question_id#347L)]
   :           +- InMemoryRelation [question_id#347L, month#375, cnt#391L], StorageLevel(disk, memory, deserialized, 1 replicas)
   :                 +- *(2) HashAggregate(keys=[question_id#347L, month#375], functions=[count(1)])
   :                    +- Exchange hashpartitioning(question_id#347L, month#375, 200), ENSURE_REQUIREMENTS, [id=#206]
   :                       +- *(1) HashAggregate(keys=[question_id#347L, month#375], functions=[partial_count(1)])
   :                          +- *(1) Project [question_id#347L, month(cast(creation_date#349 as date)) AS month#375]
   :                             +- *(1) ColumnarToRow
   :                                +- FileScan parquet [question_id#347L,creation_date#349] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/Users/mallory/Desktop/DataEngineering/Springboard/DistributedComputing/Sp..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<question_id:bigint,creation_date:timestamp>
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [id=#302]
      +- *(1) Filter isnotnull(question_id#359L)
         +- *(1) ColumnarToRow
            +- FileScan parquet [question_id#359L,creation_date#361,title#362] Batched: true, DataFilters: [isnotnull(question_id#359L)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/mallory/Desktop/DataEngineering/Springboard/DistributedComputing/Sp..., PartitionFilters: [], PushedFilters: [IsNotNull(question_id)], ReadSchema: struct<question_id:bigint,creation_date:timestamp,title:string>
```