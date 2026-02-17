## Configuration of airflow connection for MongoHook
![connection_config_1.png](images/connection_config_1.png)
![connection_config_2.png](images/connection_config_2.png)
-----
## Execution of the first DAG
![first_dag_1.png](images/first_dag_1.png)
![first_dag_2.png](images/first_dag_2.png)
-----
## ## Execution of the second DAG
![second_dag_1.png](images/second_dag_1.png)
![second_dag_2.png](images/second_dag_2.png)
------
## Data appeared in MongoDB
![mongo_data_exists.png](images/mongo_data_exists.png)
------
## Query 1
```javascript
db.reviews.aggregate([
  {
    $group: {
      _id: "$content",
      count: { $sum: 1 }
    }
  },
  { $sort: { count: -1 } },
  { $limit: 5 }
])
```
![img_5.png](images/query_1.png)
------
## Query 2
```javascript
db.reviews.find({
  $expr: {
    $lt: [{ $strLenCP: "$content" }, 5]
  }
})
```
![img_6.png](images/query_2.png)
------
## Query 3
```javascript
db.reviews.aggregate([
  {
    $group: {
      _id: {
        $dateTrunc: {
          date: "$created_at",
          unit: "day"
        }
      },
      avg_score: { $avg: "$score" }
    }
  },
  { $sort: { _id: 1 } }
])
```
![img_7.png](images/query_3.png)
------