# https://rstudio-pubs-static.s3.amazonaws.com/33876_1d7794d9a86647ca90c4f182df93f0e8.html
packages <- function(x) {
    x <- as.character(match.call()[[2]])
    if (!require(x,character.only=TRUE)) {
        install.packages(pkgs=x,repos="http://cran.r-project.org")
        require(x,character.only=TRUE)
    }
}
packages(RSQLite)
packages(cluster)
setwd("/analysis/brunch")
sqlite <- dbDriver("SQLite")
conn <- dbConnect(sqlite,"data/cluster_sample_trans.sqlite")

cluster_sample = dbGetQuery( conn,"
select item_cd
     , gap_1507 / sum_gap as ratio_1507
     , gap_1508 / sum_gap as ratio_1508
     , gap_1509 / sum_gap as ratio_1509
     , gap_1510 / sum_gap as ratio_1510
     , gap_1511 / sum_gap as ratio_1511
     , gap_1512 / sum_gap as ratio_1512
     , gap_1601 / sum_gap as ratio_1601
     , gap_1602 / sum_gap as ratio_1602
     , gap_1603 / sum_gap as ratio_1603
     , gap_1604 / sum_gap as ratio_1604
     , gap_1605 / sum_gap as ratio_1605
     , gap_1606 / sum_gap as ratio_1606
     , gap_1607 / sum_gap as ratio_1607
     , gap_1608 / sum_gap as ratio_1608
     , gap_1609 / sum_gap as ratio_1609
     , gap_1610 / sum_gap as ratio_1610
     , gap_1611 / sum_gap as ratio_1611
     , gap_1612 / sum_gap as ratio_1612
     , gap_1701 / sum_gap as ratio_1701
     , gap_1702 / sum_gap as ratio_1702
     , gap_1703 / sum_gap as ratio_1703
 from (
        select item_cd
             , item_name
             , abs(avg_price_201507 - avg_price_201506) as gap_1507
             , abs(avg_price_201508 - avg_price_201507) as gap_1508
             , abs(avg_price_201509 - avg_price_201508) as gap_1509
             , abs(avg_price_201510 - avg_price_201509) as gap_1510
             , abs(avg_price_201511 - avg_price_201510) as gap_1511
             , abs(avg_price_201512 - avg_price_201511) as gap_1512
             , abs(avg_price_201601 - avg_price_201512) as gap_1601
             , abs(avg_price_201602 - avg_price_201601) as gap_1602
             , abs(avg_price_201603 - avg_price_201602) as gap_1603
             , abs(avg_price_201604 - avg_price_201603) as gap_1604
             , abs(avg_price_201605 - avg_price_201604) as gap_1605
             , abs(avg_price_201606 - avg_price_201605) as gap_1606
             , abs(avg_price_201607 - avg_price_201606) as gap_1607
             , abs(avg_price_201608 - avg_price_201607) as gap_1608
             , abs(avg_price_201609 - avg_price_201608) as gap_1609
             , abs(avg_price_201610 - avg_price_201609) as gap_1610
             , abs(avg_price_201611 - avg_price_201610) as gap_1611
             , abs(avg_price_201612 - avg_price_201611) as gap_1612
             , abs(avg_price_201701 - avg_price_201612) as gap_1701
             , abs(avg_price_201702 - avg_price_201701) as gap_1702
             , abs(avg_price_201703 - avg_price_201702) as gap_1703

             , abs(avg_price_201507 - avg_price_201506)
             + abs(avg_price_201508 - avg_price_201507)
             + abs(avg_price_201509 - avg_price_201508)
             + abs(avg_price_201510 - avg_price_201509)
             + abs(avg_price_201511 - avg_price_201510)
             + abs(avg_price_201512 - avg_price_201511)
             + abs(avg_price_201601 - avg_price_201512)
             + abs(avg_price_201602 - avg_price_201601)
             + abs(avg_price_201603 - avg_price_201602)
             + abs(avg_price_201604 - avg_price_201603)
             + abs(avg_price_201605 - avg_price_201604)
             + abs(avg_price_201606 - avg_price_201605)
             + abs(avg_price_201607 - avg_price_201606)
             + abs(avg_price_201608 - avg_price_201607)
             + abs(avg_price_201609 - avg_price_201608)
             + abs(avg_price_201610 - avg_price_201609)
             + abs(avg_price_201611 - avg_price_201610)
             + abs(avg_price_201612 - avg_price_201611)
             + abs(avg_price_201701 - avg_price_201612)
             + abs(avg_price_201702 - avg_price_201701)
             + abs(avg_price_201703 - avg_price_201702) as sum_gap
          from (
                select item_cd
                     , item_name
                     , avg(case when substr(item_ymd,1,6) = '201506' then item_price else 0 end) as avg_price_201506
                     , avg(case when substr(item_ymd,1,6) = '201507' then item_price else 0 end) as avg_price_201507
                     , avg(case when substr(item_ymd,1,6) = '201508' then item_price else 0 end) as avg_price_201508
                     , avg(case when substr(item_ymd,1,6) = '201509' then item_price else 0 end) as avg_price_201509
                     , avg(case when substr(item_ymd,1,6) = '201510' then item_price else 0 end) as avg_price_201510
                     , avg(case when substr(item_ymd,1,6) = '201511' then item_price else 0 end) as avg_price_201511
                     , avg(case when substr(item_ymd,1,6) = '201512' then item_price else 0 end) as avg_price_201512
                     , avg(case when substr(item_ymd,1,6) = '201601' then item_price else 0 end) as avg_price_201601
                     , avg(case when substr(item_ymd,1,6) = '201602' then item_price else 0 end) as avg_price_201602
                     , avg(case when substr(item_ymd,1,6) = '201603' then item_price else 0 end) as avg_price_201603
                     , avg(case when substr(item_ymd,1,6) = '201604' then item_price else 0 end) as avg_price_201604
                     , avg(case when substr(item_ymd,1,6) = '201605' then item_price else 0 end) as avg_price_201605
                     , avg(case when substr(item_ymd,1,6) = '201606' then item_price else 0 end) as avg_price_201606
                     , avg(case when substr(item_ymd,1,6) = '201607' then item_price else 0 end) as avg_price_201607
                     , avg(case when substr(item_ymd,1,6) = '201608' then item_price else 0 end) as avg_price_201608
                     , avg(case when substr(item_ymd,1,6) = '201609' then item_price else 0 end) as avg_price_201609
                     , avg(case when substr(item_ymd,1,6) = '201610' then item_price else 0 end) as avg_price_201610
                     , avg(case when substr(item_ymd,1,6) = '201611' then item_price else 0 end) as avg_price_201611
                     , avg(case when substr(item_ymd,1,6) = '201612' then item_price else 0 end) as avg_price_201612
                     , avg(case when substr(item_ymd,1,6) = '201701' then item_price else 0 end) as avg_price_201701
                     , avg(case when substr(item_ymd,1,6) = '201702' then item_price else 0 end) as avg_price_201702
                     , avg(case when substr(item_ymd,1,6) = '201703' then item_price else 0 end) as avg_price_201703
                  from base_prices
                 group by item_cd
                        , item_name
                ) aa
        ) bb
")

cluster_sample.stand <- cluster_sample[-1]
summary(cluster_sample.stand)

k.means.fit <- kmeans(cluster_sample.stand, 3)
summary(k.means.fit)
attributes(k.means.fit)
print(k.means.fit$cluster)
print(k.means.fit$centers)
print(k.means.fit$size)

wssplot <- function(data, nc=15, seed=1234){
    wss <- (nrow(data)-1)*sum(apply(data,2,var))
    for (i in 2:nc){
        set.seed(seed)
        wss[i] <- sum(kmeans(data, centers=i)$withinss)}
    plot(1:nc, wss, type="b", xlab="Number of Clusters",
         ylab="Within groups sum of squares")}

wssplot(cluster_sample.stand, nc=3)

clusplot(cluster_sample.stand, k.means.fit$cluster, main='2D representation of the Cluster solution',
         color=TRUE, shade=TRUE,
         labels=2, lines=0)

d <- dist(cluster_sample.stand, method = "euclidean") 
H.fit <- hclust(d, method="ward")
plot(H.fit) 
groups <- cutree(H.fit, k=3)
rect.hclust(H.fit, k=3, border="red") 

result <- as.data.frame(table(cluster_sample[,1],groups))



