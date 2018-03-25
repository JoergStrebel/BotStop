require("reshape2")
require("RPostgreSQL")
require(ggplot2)
require("htmlTable")
library("tm")
library("SnowballC")
library("wordcloud")
library("RColorBrewer")
 
# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")

# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = "twitter",
                 host = "localhost", port = 5432,
                 user = "dbuser", password = "")
 
# check for the cartable
#dbExistsTable(con, c("timeline","t_status"))

################################################################################
# Zeitraum des Datenabzugs pro Thema, nulltes Chart
#
################################################################################
# query the data from postgreSQL 
dfquery1 <- dbGetQuery(con, "select t1.track_topics, to_char(min(t2.recorded_at), 'DD.MM.YYYY') startdatum, to_char(max(t2.recorded_at),'DD.MM.YYYY') enddatum,count(distinct t2.ID) anztweets, count(distinct t2.status_user_id) anzuser from datacollector.t_datacollparameter t1 inner join datacollector.t_status t2 on t1.id=t2.datacollsession_id group by t1.track_topics;")

# Spaltennamen setzen
colnames(dfquery1)<- c("Thema (Hashtag)","Startdatum","Enddatum", "Anzahl aller Tweets", "Anzahl unterschiedlicher Nutzer")

# 'table' ist eine Bootstrap CSS-Klasse
strHTMLTable<-htmlTable(dfquery1,align = "lrrrr",align.header = "lllll",col.rgroup = c("none", "#F7F7F7"),css.table="table", css.class="table") 

# schreibe in Datei. Leider ist der Zeichensatz unbekannt...ich hoffe es ist UTF-8
cat(strHTMLTable,file = "statistics.html", sep = "", fill = FALSE, labels = NULL, append = FALSE)


################################################################################
# User vs Bots, erstes Chart
#
################################################################################
# query the data from postgreSQL
# Kommas werden durch Newline ersetzt, damit ggplot2 die Hashtags untereinander schreibt
dfpostgres <- dbGetQuery(con, "select replace(track_topics,',','\n') track_topics, useranzahl, botanzahl from timeline.mv_analytics_bot_per_session")

# TODO: Stand 16.06.2017 wird das #dumptrump Thema nicht ausgewertet, da scheint noch ein Defect in der Botscore-Berechnung zu sein.
mdata <- melt(dfpostgres[c(1,2),], id=c("track_topics"))
#mdata <- melt(dfpostgres, id=c("track_topics"))
head(mdata)
# draw barchart

# Basic barplot
p<-ggplot(data=mdata, aes(x=track_topics, y=value, fill=variable))
p<-p + geom_bar(stat="identity", position=position_dodge())
p<-p + geom_text(aes(label=value), vjust=-0.3, color="black", position = position_dodge(0.9), size=5)
p<-p + scale_fill_brewer(palette="Paired",labels=c("User", "Bots"))
p<-p + theme_minimal(base_size = 16)
p<-p + ylab("Anzahl User")+xlab("")
p<-p + theme(legend.title=element_blank(), axis.text.x = element_text(size=16), axis.text.y = element_text(size=16))

svg("botspertopic.svg",width=10,height=5)
p
dev.off()
   
# Horizontal bar plot
#p + coord_flip()

################################################################################
# Zeitverlauf Tweets, User, Bots
# zweites Chart
#
################################################################################
# query the data from postgreSQL 
df2 <- dbGetQuery(con, "select tweetdatum, Tweetanzahl, BotTweetAnzahl from timeline.MV_ANALYTICS_COUNT_PER_DAY where track_topics='#AfD'")
colnames(df2)<- c("tweetdatum","Anzahl aller Tweets zur AfD","Anzahl der Tweets von Bots")

# draw linechart over timeline, long data form
mdata <- melt(df2, id=c("tweetdatum"))
head(mdata)
str(mdata)
p2<-ggplot(data=mdata, aes(x=tweetdatum, y=value, group=variable, colour=variable))
p2<-p2 + geom_line()
p2<-p2 + geom_vline(xintercept=as.numeric(as.POSIXct('2017-04-22')), show.legend=FALSE, color="red")
p2<-p2 + geom_point()
p2<-p2 + scale_color_brewer(palette="Paired")
p2<-p2 + theme_minimal(base_size = 16)
p2<-p2 + ylab("Anzahl Tweets")+xlab("")
p2<-p2 + theme(legend.title=element_blank(), axis.text.x = element_text(size=16), axis.text.y = element_text(size=16))
p2<-p2 + geom_label(x=as.numeric(as.POSIXct('2017-04-18')), y=31000, label="AfD Parteitag am 22.04.2017", family = "Arial", color="red", show.legend=FALSE, size=5)

svg("afdbotsovertime.svg",width=10,height=5)
p2
dev.off()


##########################################
# Test - Bar Chart Integration
# Sehr frickelig, da ich manuell eine zweite Achse anlegen muss, die Legende auch selbst erzeugen muss und die Farbgestaltung auch manuell gesetzt werden muss.
# Darstellugn wird dadurch eher unübersichtlich
#########################################
#p2<-ggplot(data=df2)
#p2<-p2 + geom_bar(stat="identity",aes_string(x=as.name(names(df2)[1]), y=as.name(names(df2)[4])),show.legend=TRUE)
#p2<-p2 + geom_line(aes_string(x=as.name(names(df2)[1]), y=as.name(names(df2)[2])),show.legend=TRUE)
#p2<-p2 + geom_line(aes_string(x=as.name(names(df2)[1]), y=as.name(names(df2)[3])),show.legend=TRUE)
#p2<-p2 + geom_vline(xintercept=as.numeric(as.POSIXct('2017-04-22')), show.legend=FALSE, color="red")
#p2<-p2 + geom_point(aes_string(x=as.name(names(df2)[1]), y="Anzahl aller Tweets zur AfD", group="Anzahl aller Tweets zur AfD"))
#p2<-p2 + scale_color_brewer(palette="Paired")
#p2<-p2 + theme_minimal(base_size = 16)
#p2<-p2 + ylab("Anzahl Tweets")+xlab("")
#p2<-p2 + theme(legend.title=element_blank(), axis.text.x = element_text(size=16), axis.text.y = element_text(size=16))
#p2<-p2 + geom_label(x=as.numeric(as.POSIXct('2017-04-18')), y=31000, label="AfD Parteitag am 22.04.2017", family = "Arial", color="red", show.legend=FALSE, size=5)

#svg("afdbotsovertime.svg",width=10,height=5)
#p2
#dev.off()

################################################################################
# Histogramm zu Botscore Stufe 1
# drittes Chart
#
################################################################################
df3 <- dbGetQuery(con, "select totalbotscore,case when totalbotscore>0.5 then 'Bot' else 'Kein Bot' end botflag from datacollector.V_ANALYTICS_ALLBOTSCORES_STUFE1")
colnames(df3)<- c("BotScore Stufe 1","Bot-Kennzeichen")

p3 <- ggplot(data=df3, aes_string(x=as.name(names(df3)[1]),fill=as.name(names(df3)[2])))
p3 <- p3 + scale_fill_brewer(palette="Paired")
p3 <- p3 + geom_histogram(breaks=seq(0, 1, by = 0.01),color="darkblue")
p3 <- p3 + theme_minimal(base_size = 16)
p3 <- p3 + ylab("Anzahl User")+xlab("BotScore Stufe 1")
p3 <- p3 + theme(legend.title=element_blank(), axis.text.x = element_text(size=16), axis.text.y = element_text(size=16))

svg("botscorehisto_stufe1.svg",width=10,height=5)
p3
dev.off()

################################################################################
# Histogramm zu Botscore Stufe 2
# viertes Chart
#
################################################################################
df4 <- dbGetQuery(con, "select totalbotscore,case when totalbotscore>0.5 then 'Bot' else 'Kein Bot' end botflag from timeline.MV_ANALYTICS_ALLBOTSCORES_STUFE2")
colnames(df4)<- c("BotScore Stufe 2","Bot-Kennzeichen")

p4 <- ggplot(data=df4, aes_string(x=as.name(names(df4)[1]),fill=as.name(names(df4)[2])))
p4 <- p4 + scale_fill_brewer(palette="Paired")
p4 <- p4 + geom_histogram(breaks=seq(0, 1, by = 0.01),color="darkblue")
p4 <- p4 + theme_minimal(base_size = 16)
p4 <- p4 + ylab("Anzahl User")+xlab("BotScore Stufe 2")
p4 <- p4 + theme(legend.title=element_blank(), axis.text.x = element_text(size=16), axis.text.y = element_text(size=16))

svg("botscorehisto_stufe2.svg",width=10,height=5)
p4
dev.off()

################################################################################
# Word Cloud für die Themen
# TODO: Wieviele Zeichen bekomme ich denn für jede Word Cloud aus der DB tatsächlich? Wenn ich zu wenige Zeichen für Bots bekomme, dann erklärt das den Unterschied freilich.
#
################################################################################
# Daten holen. Ich werde alle Tweets pro Thema zusammenfassen zu einem großen String.   
dfwordcloud <- dbGetQuery(con, "select track_topics, bottweets, notbottweets from timeline.V_ANALYTICS_WORDS")

# Hier kommt die Wordclodud für Bot-Wörter:
# nimm die erste Zeile, da steht AfD, in der zweiten Spalte stehen die Botwörter
docs <- SimpleCorpus(VectorSource(dfwordcloud[c(1),c(2)]), control = list(language = "de"))
#str(docs)
#summary(docs)
#inspect(docs)

# Gemeinsame Stop-Words
vStopWords <- c("tco","nein","los","afd","erst","the","you", "dass")

# Werfe bestimmte Muster aus dem Text
toSpace <- content_transformer(function (x , pattern ) gsub(pattern, " ", x))
docs <- tm_map(docs, toSpace, "/")
docs <- tm_map(docs, toSpace, "@")
docs <- tm_map(docs, toSpace, "\\|")
docs <- tm_map(docs, toSpace, "#afd")
docs <- tm_map(docs, toSpace, "#")
docs <- tm_map(docs, toSpace, "https")
docs <- tm_map(docs, toSpace, "http")
docs <- tm_map(docs, toSpace, "tco")
#docs <- tm_map(docs, toSpace, ":)")
#docs <- tm_map(docs, toSpace, ":(")
docs <- tm_map(docs, toSpace, "^^")
docs <- tm_map(docs, toSpace, "\\.\\.\\.")

# Eliminate extra white spaces
docs <- tm_map(docs, stripWhitespace)

# Remove numbers
docs <- tm_map(docs, removeNumbers)
# Remove english common stopwords
docs <- tm_map(docs, removeWords, stopwords("german"))
# Remove punctuations
docs <- tm_map(docs, removePunctuation)
# Remove your own stop word
# specify your stopwords as a character vector
 docs <- tm_map(docs, removeWords, vStopWords) 
# Text stemming
docs <- tm_map(docs, stemDocument)

dtm <- TermDocumentMatrix(docs)
m <- as.matrix(dtm)
v <- sort(rowSums(m),decreasing=TRUE)
d <- data.frame(word = names(v),freq=v)
head(d, 10)

set.seed(1234)

png("afdwordcloudbot.png",width=600,height=600,antialias="subpixel",pointsize = 16)
wordcloud(words = d$word, freq = d$freq, min.freq = 3,
          max.words=200, random.order=FALSE, rot.per=0.35, 
          colors=brewer.pal(8, "Dark2"))
dev.off()

# Hier kommt die Wordclodud für Nicht-Bot-Wörter:
# nimm die erste Zeile, da steht AfD
docs <- SimpleCorpus(VectorSource(dfwordcloud[c(1),c(3)]), control = list(language = "de"))

toSpace <- content_transformer(function (x , pattern ) gsub(pattern, " ", x))
docs <- tm_map(docs, toSpace, "/")
docs <- tm_map(docs, toSpace, "@")
docs <- tm_map(docs, toSpace, "\\|")
docs <- tm_map(docs, toSpace, "#afd")
docs <- tm_map(docs, toSpace, "#")
docs <- tm_map(docs, toSpace, "https")
docs <- tm_map(docs, toSpace, "http")
docs <- tm_map(docs, toSpace, "tco")
#docs <- tm_map(docs, toSpace, ":)")
#docs <- tm_map(docs, toSpace, ":(")
docs <- tm_map(docs, toSpace, "^^")
docs <- tm_map(docs, toSpace, "\\.\\.\\.")

# Eliminate extra white spaces
docs <- tm_map(docs, stripWhitespace)

# Remove numbers
docs <- tm_map(docs, removeNumbers)
# Remove english common stopwords
docs <- tm_map(docs, removeWords, stopwords("german"))
# Remove punctuations
docs <- tm_map(docs, removePunctuation)
# Remove your own stop word
# specify your stopwords as a character vector
 docs <- tm_map(docs, removeWords, vStopWords) 
# Text stemming
docs <- tm_map(docs, stemDocument)

dtm <- TermDocumentMatrix(docs)
m <- as.matrix(dtm)
v <- sort(rowSums(m),decreasing=TRUE)
d <- data.frame(word = names(v),freq=v)
head(d, 10)

set.seed(1234)

png("afdwordcloudother.png",width=600,height=600,antialias="subpixel",pointsize = 16)
wordcloud(words = d$word, freq = d$freq, min.freq = 3,
          max.words=200, random.order=FALSE, rot.per=0.35, 
          colors=brewer.pal(8, "Dark2"))
dev.off()


# close the connection
dbDisconnect(con)
dbUnloadDriver(drv)
