## RedWineQuality-Prediction

#Hive basic commands:

show databases;--->displays the existing db
describe database db name;----->displays the location of the db
use db_name;--->to use the db

ctrl+l--->clear the hive screen

1.)table creation:
create table winequality(fixed_acidity float,volatile_acid float,citric_acid float,res_sugar int,chlorides float,free_so2 int,density float,ph float,sulphates float, quality int,taste string,phrange string, alcohol int) row format delimited fields terminated by ',' stored as textfile;

2.)loading data from local file:
load data local inpath '/home/Desktop/winequality.csv' overwrite into table winequality;

3.) select * from winequality;


#Hadoop:
For Hadoop part refer the below blog link and watch sample youtube video realted to how to run hadoop environment in my youtube channel.
Youtube link: https://youtu.be/XWq4wWaCQWg
Blog link:

