# Milestone 1, Load and Simple Analysis
## Loaders
`MoviesLoader` and `RatingsLoader` loads files from `./src/main/resources/`
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

Although it is said the there won't be any duplicated (UserID, MovieID) pairs, it works assuming there exist some and sorted each person's rating for the same movie chronogically, to add the previous rating entry. 

# Milestone 2, Aggregate the ratings, Maintain the average
## Aggregator
`init()` computes the average rating for each movie, according to the original ratings.

### aggrageted
| Property | Type | Description |
|-|-|-|
| | RDD[(Int, (String, List[String], List[(Int, List[(Int, Option[Double], Double, Int)])], Double, Int))] | commentes aggragated by MovieID, then a tuple of data, describe the information of this movie, the comments, and the statistic data of this movie |
| _._1 | Int | MovieID |
| _._2._1 | String | the title of this movie |
| _._2._2 | List[String] | the keywords of this movie |
| _._2._3 | List[(Int, List[(Int, Option[Double], Double, Int)])] | the ratings of this movie, the inner format is the same as described in `ratingsGroupedByYearByTitle` |
| _._2._4 | Double | Sum of ratings given by all users |
| _._2._5 | Int | Number of available ratings, multiple ratings from the same user only counted once |

### GetKeywordsQueryResult
First filter the aggregated movies by keywords, then filter by number of ratings, finally return the average of movies left

### updateResult
As I observed, the comments themselves are not useful, only the statistic data counts. 
During this process, I only update the `sum` and `count` parts of `aggregated`

First group the deltaRatings with `MovieId`, then join it from `aggregated`, finally process those updated movies

When adding new comments to the specific movie, check it's `previous rating` part and update the `sum` accordingly. To notice, increase `count` only when the `previous` is `None`


# Milestone 3, Calculate User's Preference, Recommend Similar Movies

## LSH
Use hash conflict to find out the similar keywords combinations

Maybe using `zip()` is much faster than `join()` ?

## Baseline
Simply use average data to guess users' preference. 

| Var | Type | Description |
|-|-|-|
| ratingUserBased | RDD[(Int, Double)] | the average rating of each user |
| ratingMovieBased | RDD[(Int, Double)] |the average rating of each user |
| ratingUserMovie | RDD[(Int, List[(Int, Double)])] | the rating grouped by UserID, then each item of the list contains MovieID and the corresponding rating |

### init()
Simply perform the normalization provided by the instruction

### listWatchedMovies()
This is the correct part to provide the movies watched by one specific user, for the latter recommendation

## collaborativeFiltering
Use the library of ALS, and the previous ratings as the training dataset. 
Return predicted ratings for given UserID and MovieID

## Recommender
For a given user and given keywords, return a list of recommended movies

First get the movie list using LSH, then filter it if already watched. 
Sort them according to the predicted ratings decreasingly, then return the top K movies. 