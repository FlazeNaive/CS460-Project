## SimpleAnalytics
`init()` creates two partitioned lists, `titlesGroupedByID` and `ratingsGroupedByYearByTitle`

### titlesGroupedByID
| Property | Descriptioni |
|---|---|
| Type | RDD[(Int, Iterable[(String, List[String])])] |
| Int | ID |
| String | title |
| List[String] | tags |

### ratingsGroupedByYearByTitle
| Property | Descriptioni |
|---|---|
| Type | RDD[(_1: Int, _2: Map[_1: Int, _2: Iterable[(_1: Int, _2: Option[Double], _3: Double, _4: Int)]])] |
| _1 | Year |
| _2 | Ratings this year |
| _2._1 | ID for Movie |
| _2._2 | Ratings for this movie, this year |
| _2._2._1 | User ID |
| _2._2._2 | Previous rating (if appliable) |
| _2._2._3 | Current rating |
| _2._2._4 | Timestamp |