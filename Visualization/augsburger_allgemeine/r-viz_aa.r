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
dfquery1 <- dbGetQuery(con, "select 'Follower der Augsburger Allgemeinen' track_topics, to_char(min(t2.created_at), 'DD.MM.YYYY') startdatum, to_char(max(t2.created_at),'DD.MM.YYYY') enddatum,count(distinct t2.ID) anztweets, count(distinct t2.status_user_id) anzuser from  timeline.t_status t2;")

# Spaltennamen setzen
colnames(dfquery1)<- c("Thema (Hashtag)","Jüngster Tweet","Ältester Tweet", "Anzahl aller Tweets", "Anzahl unterschiedlicher Nutzer")

# 'table' ist eine Bootstrap CSS-Klasse
strHTMLTable<-htmlTable(dfquery1,align = "lrrrr",align.header = "lllll",col.rgroup = c("none", "#F7F7F7"),css.table="table", css.class="table") 

# schreibe in Datei. Leider ist der Zeichensatz unbekannt...ich hoffe es ist UTF-8
cat(strHTMLTable,file = "aa_statistics.html", sep = "", fill = FALSE, labels = NULL, append = FALSE)


################################################################################
# Histogramm zu Botscore
#
################################################################################
df3 <- dbGetQuery(con, "select botscore,case when botscore>0.5 then 'Bot' else 'Kein Bot' end botflag from timeline.t_challenge_user")
colnames(df3)<- c("BotScore","Bot-Kennzeichen")

p3 <- ggplot(data=df3, aes_string(x=as.name(names(df3)[1]),fill=as.name(names(df3)[2])))
p3 <- p3 + scale_fill_brewer(palette="Paired")
p3 <- p3 + geom_histogram(breaks=seq(0, 1, by = 0.01),color="darkblue")
p3 <- p3 + theme_minimal(base_size = 16)
p3 <- p3 + ylab("Anzahl User")+xlab("BotScore")
p3 <- p3 + theme(legend.title=element_blank(), axis.text.x = element_text(size=16), axis.text.y = element_text(size=16))

svg("aa_botscore_histo.svg",width=10,height=5)
p3
dev.off()


################################################################################
# Word Cloud für die Themen
#
################################################################################
# Daten holen. Ich werde alle Tweets pro Thema zusammenfassen zu einem großen String.   
dfwordcloud <- dbGetQuery(con, "select 'Follower der Augsburger Allgemeinen' track_topics, bottweets, notbottweets from timeline.V_ANALYTICS_WORDS")

# Hier kommt die Wordclodud für Bot-Wörter:
# nimm die erste Zeile, da steht AfD, in der zweiten Spalte stehen die Botwörter
docs <- SimpleCorpus(VectorSource(dfwordcloud[c(1),c(2)]), control = list(language = "de"))
#str(docs)
#summary(docs)
#inspect(docs)

# Gemeinsame Stop-Words
vStopWords <- c("tco","nein","los","erst","the","you", "dass", "and", "for" , "mal", "via", "with", "https", "http", "this")

# Werfe bestimmte Muster aus dem Text
toSpace <- content_transformer(function (x , pattern ) gsub(pattern, " ", x))
#docs <- tm_map(docs, toSpace, "http(s?)://(.*)[.][a-z]+/[A-Za-z0-9]+")
docs <- tm_map(docs, toSpace, "https://t.co/")
docs <- tm_map(docs, toSpace, "http://t.co/")
docs <- tm_map(docs, toSpace, "http://")
docs <- tm_map(docs, toSpace, "https://")
docs <- tm_map(docs, toSpace, "@")
docs <- tm_map(docs, toSpace, "\\|")
docs <- tm_map(docs, toSpace, "#")
docs <- tm_map(docs, toSpace, "\\.\\.\\.")

# Eliminate extra white spaces
docs <- tm_map(docs, stripWhitespace)

# Remove numbers
docs <- tm_map(docs, removeNumbers)
# Remove common stopwords
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

png("aa_wordcloudbot.png",width=600,height=600,antialias="subpixel",pointsize = 16)
wordcloud(words = d$word, freq = d$freq, min.freq = 3,
          max.words=200, random.order=FALSE, rot.per=0.35, 
          colors=brewer.pal(8, "Dark2"))
dev.off()

# Hier kommt die Wordcloud für Nicht-Bot-Wörter:
docs <- SimpleCorpus(VectorSource(dfwordcloud[c(1),c(3)]), control = list(language = "de"))

toSpace <- content_transformer(function (x , pattern ) gsub(pattern, " ", x))
#docs <- tm_map(docs, toSpace, "http(s?)://(.*)[.][a-z]+/[A-Za-z0-9]+")
docs <- tm_map(docs, toSpace, "https://t.co/")
docs <- tm_map(docs, toSpace, "http://t.co/")
docs <- tm_map(docs, toSpace, "http://")
docs <- tm_map(docs, toSpace, "https://")
docs <- tm_map(docs, toSpace, "@")
docs <- tm_map(docs, toSpace, "\\|")
docs <- tm_map(docs, toSpace, "#")
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

png("aa_wordcloudother.png",width=600,height=600,antialias="subpixel",pointsize = 16)
wordcloud(words = d$word, freq = d$freq, min.freq = 3,
          max.words=200, random.order=FALSE, rot.per=0.35, 
          colors=brewer.pal(8, "Dark2"))
dev.off()


# close the connection
#dbDisconnect(con)
#dbUnloadDriver(drv)


#xstr <- c("@zedbeeblebrox :) #tagamsee http://t.co/eekqkHzdUC #gameinsight #iphone", "@SandraStruewing #twauxx #draussen #kuscheligwarm https://t.co/ZpkzE7JcUj asdas asdasdas")
#gsub("http(s?)://(.*)[.][a-z]+/[A-Za-z0-9]+", "", xstr)
