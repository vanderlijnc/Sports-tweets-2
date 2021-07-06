#load required packages
library(foreign)
library(spatialreg)
library(spdep)
library(car)
library(AICcmodavg)
library(corrplot)

#VARIABLE PRESELECTION
#load data, drop unnecessary columns and give more descriptive column names
data <- read.dbf(file="~/Desktop/YLLI/gradu/gradu/data/FINAL_PIP.dbf")
df <- subset(data, select=-c(Posno,Toimip, Toimip_ru, Nimi, Nimi_Ru, Kunta, Kunta_nro, NUMfacilit, Tweets.per, area, Percentage, all_geotag, all_geot_1, X13., Count.of.t, pop, X18.49, X18_49_pop, keskitulo2))
names(df) <- c("tweets", "median_income", "high_education", "low_income", "high_income", "sport_facilities", "household_size", "low_education", "children", "students", "pensioners", "employed", "unemployed", "own_house", "rental", "kids_house", "adult_house", "pensioner_house")
#df <- df[ , c(18, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)] 

#print out a correlation matrix to pick the variables
M<-cor(df, use="complete.obs")
res1 <- cor.mtest(M, conf.level = .95)
corrplot(M, p.mat = res1$p, method = "color", type= "lower", addCoef.col = "white", order = "hclust", tl.col = "black", tl.srt = 45,number.cex=0.75)

#OLS 
#run OLS first with all variables and then drop variables according to OLS results and VIF score
attach(df)
OLS <- lm(tweets ~ sport_facilities + children + employed + kids_house + adult_house + own_house + rental + household_size)
summary(OLS)
vif(OLS)
AICc(OLS)

#final OLS
OLS_final <- lm(tweets ~ sport_facilities + children + employed)
summary(OLS_final)
vif(OLS_final)
AICc(OLS_final)

#LAGRANGE MULTIPLIER TESTS
#import weights created in GeoDa and convert to R format
geoda_weights<-read.gal(file="~/Desktop/YLLI/gradu/gradu/data/PIP_nobots.gal",override.id=TRUE)
weights <- nb2listw(geoda_weights)

#run LM tests to check if spatial models would be better fit than OLS
LMtests <- lm.LMtests(OLS_final, weights, test="all")
print(LMtests)

#no p-value was significant, so I keep the OLS results 





