
DATA_HOME=~/data
mkdir ~/data
#wget http://www.cms.gov/apps/ama/license.asp?file=http://download.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/Medicare_Provider_Util_Payment_PUF_CY2014.zip
wget http://download.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Downloads/Medicare_Provider_Util_Payment_PUF_CY2014.zip
mv Medicare_Provider_Util_Payment_PUF_CY2014.zip ~/data/
unzip ~/data/Medicare_Provider_Util_Payment_PUF_CY2014.zip -d ~/data/
rm ~/data/Medicare_Provider_Util_Payment_PUF_CY2014.zip  
