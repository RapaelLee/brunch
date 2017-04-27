packages <- function(x) {
  x <- as.character(match.call()[[2]])
  if (!require(x,character.only=TRUE)) {
    install.packages(pkgs=x,repos="http://cran.r-project.org")
    require(x,character.only=TRUE)
  }
}
run_ar_sample <- function(sqlite_file, work_path) {
  packages(RSQLite)
  packages(arules)
  setwd(work_path)
  sqlite <- dbDriver("SQLite")
  conn <- dbConnect(sqlite,sqlite_file)
  
  ar_sample = dbGetQuery( conn,'
select t2.item_name as item, t1.customer_id
  from ar_samples t1
 inner join (select item_cd
                  , item_name
               from base_prices
              group by item_cd
                     , item_name) t2
         on t1.item_cd = t2.item_cd
' )
  ar_list <- split(ar_sample$item, ar_sample$customer_id)
  ar_trans <- as(ar_list, "transactions")
  
  rules <- apriori(data=ar_trans, parameter = list(support = 0.01, confidence = 0.80))
  # summary(rules)
  ar_result <- as.data.frame(inspect(sort(rules, by = "confidence")))
  return(rapply(ar_result, as.character, classes="factor", how="replace"))
}
ar_result <- run_ar_sample("data/ar_sample_trans.sqlite", "/analysis/brunch")
