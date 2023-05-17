## SimpleAnalytics
`init()` creates two partitioned lists, `titlesGroupedByID` and `ratingsGroupedByYearByTitle`

### titlesGroupedByID
| Property | Description |
|---|---|
| Type | RDD[(Int, Iterable[(String, List[String])])] |
| Int | ID |
| String | title |
| List[String] | tags |

### ratingsGroupedByYearByTitle
| Property | Type | Description |
|---|---|---|
| | RDD[(_1: Int, _2: Map[_1: Int, _2: Iterable[(_1: Int, _2: Option[Double], _3: Double, _4: Int)]])] | grouped ratings |
| _1 | Int | Year |
| _2 | Map[Int, Iterable] |Ratings this year |
| _2._1 | Int |ID for Movie |
| _2._2 | Iterable[] | Ratings for this movie, this year |
| _2._2._1 | Int | User ID |
| _2._2._2 | Option[Double] |Previous rating (if appliable) |
| _2._2._3 | Double |Current rating |
| _2._2._4 | Int |Timestamp |