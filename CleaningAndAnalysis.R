
library(textcat)
library(stringr)
library(ggplot2)
library(GoodmanKruskal)
flights<-read.csv("C:/Users/Mashhood Malik/PycharmProjects/flights.csv")
flights
#Mapping rull values to 0
flights[is.na(flights)] <- 0
flights

flights1<-flights[which(!grepl("[^\x01-\x7F]+", flights$UniqueCarrier)),]
flights2<-flights1[which(!grepl("[^\x01-\x7F]+", flights1$TailNum)),]
flights3<-flights2[which(!grepl("[^\x01-\x7F]+", flights2$Origin)),]
flights4<-flights3[which(!grepl("[^\x01-\x7F]+", flights3$Dest)),]
flights4

write.csv(flights4,"C:/Users/Mashhood Malik/Desktop/flights.csv")

#Visualization of transformation
ggplot(flights, aes(flights$DepDelay))+geom_histogram(flill="blue")
ggplot(flights, aes(flights$ArrDelay))+geom_histogram(flill="blue")
flights$flights_depdelay_log<-log(flights$DepDelay)
flights$flights_arrdelay_log<-log(flights$ArrDelay)
ggplot(flights, aes(x=flights$flights_depdelay_log, fill="blue")) + geom_density(alpha=0.5)
ggplot(flights, aes(x=flights$flights_arrdelay_log, fill="blue")) + geom_density(alpha=0.5)
#Test of independence to find most related columns:
chisq.test(flights4$DayOfWeek, flights4$ArrDelay)
##Pearson's Chi-squared test

##data:  flights4$DayOfWeek and flights4$ArrDelay
##X-squared = 2224.9, df = 2004, p-value = 0.0003629

chisq.test(flights4$Month, flights4$ArrDelay)
chisq.test(flights4$CRSElapsedTime, flights4$ArrDelay)
chisq.test(flights4$DepDelay, flights4$ArrDelay)
chisq.test(flights4$TaxiOut, flights4$ArrDelay)
chisq.test(flights4$Distance, flights4$ArrDelay)

Attributes<- c("DepTime", "CRSDepTime", "CRSArrTime", "CRSElapsedTime", "DepDelay", "ArrDelay", "Distance")
DataFrame<- subset(flights4, select = Attributes)
FinalMatrix<- GKtauDataframe(DataFrame)
plot(FinalMatrix, corrColors = "blue")


#Cleaning of truncated big csv
truncated_csv<-read.csv("C:/Users/Mashhood Malik/Desktop/cleanFlights.csv")
truncated_csv<-truncated_csv[which(!grepl("[^\x01-\x7F]+", truncated_csv$TailNum)),] 
write.csv(truncated_csv, "C:/Users/Mashhood Malik/Desktop/truncated.csv")
