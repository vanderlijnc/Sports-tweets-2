#load required modules (keep the following order)
library(foreign)
library(spatialreg)
library(spdep)

#importing shapefile dbf file
data <- read.dbf(file="~/Desktop/YLLI/gradu/gradu/data/PIP_nobots.dbf")
attach(data)

#import weights created in GeoDa and convert to R format
geoda_weights<-read.gal(file="~/Desktop/YLLI/gradu/gradu/data/PIP_nobots.gal",override.id=TRUE)
weights <- nb2listw(geoda_weights)

#OLS analysis
OLS <- lm(no_cities_ ~ NUMfacilit + hi_edu + pop + keskitulo2)
summary(OLS)

#check R squared and p-value

#plot the residuals and check for spatial autocorrelation
plot(OLS$fitted.values, OLS$residuals)
moran.test(OLS$residuals, weights)
moran.plot(OLS$residuals, weights)

#run LM tests to check for better test
LMtests <- lm.LMtests(OLS, weights, test="all")
print(LMtests)

#which p-value is more significant?

# A) Spatial Error Model
SEM <- errorsarlm(NUMPOSTS ~ NUMfacilit + hi_income + hi_edu + keskitulo2, data=data, weights)
sum.SEM <- summary(SEM, Nagelkerke=TRUE)
print(sum.SEM, signif.stars=TRUE)

#preparation for SDM
W <- as(as_dgRMatrix_listw(weights), "CsparseMatrix")
tr <- trW(W, type="MC")
SDM <- lagsarlm(NUMPOSTS ~ NUMfacilit + hi_income + hi_edu + keskitulo2, data=data, weights, type="mixed", method="MC", trs=tr)

#common factor hypothesis
CFH <- LR.sarlm(SDM, SEM)
print(CFH)

#→ if p-value is significant → SDM
sum.SDM <- summary(SDM, Nagelkerke=TRUE)
print(sum.SDM, signif.stars=TRUE)
hist(SDM$residuals)
plot(SDM$fitted.values, SDM$residuals)
moran.test(SDM$residuals, weights)
moran.plot(SDM$residuals, weights)

#to go further, implement direct, indirect and total impacts


# B) Spatial Lag Model
SAR <- lagsarlm(no_cities_ ~ NUMfacilit + hi_edu + pop + mediaanitu, data=data, weights)
sum.SAR <- summary(SAR, Nagelkerke=TRUE)
print(sum.SAR, signif.stars=TRUE)
plot(SAR$fitted.values, SAR$residuals)
moran.test(SAR$residuals, weights)
moran.plot(SAR$residuals, weights)

#Residuals should not be spatially autocorrelated




