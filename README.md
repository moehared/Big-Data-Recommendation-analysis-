
# Big-Data-Recommendation-sys

this project was done for data system engineering class on Big Data. i used this dataset [books recommendation dataset](https://www.kaggle.com/arashnic/book-recommendation-dataset) found in kaggle. then i used following technology :
* [kafka] - used for reading data from csv file and creating topics. these topics subscribed by varies database such SQL and mongoDB as data sink.  
* [spark] - spark is used for anaylysing data using batch and real time analytics as well as using MLlib provided by spark to recommend books to all users.


## outputs:
### Model 1: Recommendation model without training data 
![reccomendation output 1](https://user-images.githubusercontent.com/46978582/113816540-39464e80-9732-11eb-8bda-d2ffbe814965.png)

### Model 2: Recommendation model with trained data 

![reccomendation output 2](https://user-images.githubusercontent.com/46978582/113816726-6eeb3780-9732-11eb-9cc3-f5d5324bcd25.png)

### Evaluating both model

![Evoluating both model ](https://user-images.githubusercontent.com/46978582/113816759-79a5cc80-9732-11eb-86c7-7fd08b03b178.png)


