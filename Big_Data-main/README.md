# MAPREDUCE

- ### File hướng dẫn: [Map_reduce.pdf](./Map_Reduce.pdf)

# TRUY VẤN PIG LATIN

- ### File code crawl data và mapreduce: [Code](Code/)
- ### Tải và xem nội cấu trúc bằng file excel: [books.xlsx](Data/books.xlsx)
- ### Tải và xem nội cấu trúc bằng file csv: [books.csv](Data/books.csv)
- ### Tải và xem nội cấu trúc bằng file json: [books.json](Data/books.json)

## Yêu cầu:

### 1. Sau khi crawl dữ liệu nên xuất ra file excel có cấu trúc các trường cách nhau bởi dấu `$` như sau:

```bash
title$img_url$rating$price$status$desc$upc$product_type$price_excl$price_incl$tax$availability$number_of_reviews$type_of_book
```

### 2. Để chạy được các lệnh hdfs và pig thì cần chạy lệnh sau:

```bash
start-all.sh
```

## Chuẩn bị:

### 1. Lưu dữ liệu từ local lên hdfs

```
hdfs dfs -put books.csv /books
```

### 2. Kiểm tra dữ liệu đã được lưu lên hdfs chưa

```
hdfs dfs -ls /books
```

## Hướng dẫn:

### Để chạy được lệnh pig thì cần chạy lệnh sau:

```bash
pig -x mapreduce
```

### 1. Đọc dữ liệu từ tệp books.csv với dấu phân cách là `$` và chuyển về các trường chính xác

```
books = LOAD '/books/books.csv' USING PigStorage('$') AS (title:chararray, img_url:chararray, rating:chararray, price:double, status:chararray, desc:chararray, upc:chararray, product_type:chararray, price_excl:double, price_incl:double, tax:double, availability:int, number_of_reviews:int, type_of_book:chararray);
```

### 2. Đếm số lượng sách

```
grouped_books = GROUP books ALL;
```
```
book_count = FOREACH grouped_books GENERATE COUNT(books) AS total_books;
```

```
DUMP book_count;
```

### 3. Lọc các sách có giá trên 50

```
expensive_books = FILTER books BY (price > 50.0);
```

```
DUMP expensive_books;
```

### 4. Lọc các sách có giá dưới 50

```
cheap_books = FILTER books BY (price < 50.0);
```

```
DUMP cheap_books;
```

### 5. Lọc các sách tồn kho > 10

```
available_books = FILTER books BY (availability > 10);
```

```
DUMP available_books;
```

### 6. Lọc các sách có số lượng đánh giá > 100

```
popular_books = FILTER books BY (number_of_reviews > 100);
```

```
DUMP popular_books;
```

### 7. Lọc các sách có số lượng đánh giá > 100 và giá trên 50

```
popular_expensive_books = FILTER books BY (number_of_reviews > 100 AND price > 50.0);
```

```
DUMP popular_expensive_books;
```

### 8. 5 sách có giá cao nhất

```
top_5_expensive_books = ORDER books BY price DESC;
top_5_expensive_books = LIMIT top_5_expensive_books 5;
```

```
DUMP top_5_expensive_books;
```

### 9. 5 sách có giá thấp nhất

```
top_5_cheap_books = ORDER books BY price ASC;
top_5_cheap_books = LIMIT top_5_cheap_books 5;
```

```
DUMP top_5_cheap_books;
```

### 10. Sắp xếp sách theo số lượng đánh giá giảm dần

```
books_by_reviews = ORDER books BY number_of_reviews DESC;
```

```
DUMP books_by_reviews;
```

### 11. Sắp xếp sách theo số lượng đánh giá tăng dần

```
books_by_reviews = ORDER books BY number_of_reviews ASC;
```

```
DUMP books_by_reviews;
```

### 12. Sắp xếp sách theo giá tăng dần

```
books_by_price = ORDER books BY price ASC;
```

```
DUMP books_by_price;
```

### 13. Sắp xếp sách theo giá giảm dần

```
books_by_price = ORDER books BY price DESC;
```

```
DUMP books_by_price;
```

### 14. Sắp xếp sách theo giá tăng dần và số lượng đánh giá giảm dần

```
books_by_price_reviews = ORDER books BY price ASC, number_of_reviews DESC;
```

```
DUMP books_by_price_reviews;
```

### 15. Sắp xếp sách theo giá giảm dần và số lượng đánh giá tăng dần

```
books_by_price_reviews = ORDER books BY price DESC, number_of_reviews ASC;
```

```
DUMP books_by_price_reviews;
```

### 16. Tổng tiền của tất cả sách

```
total_price = FOREACH books GENERATE SUM(price) AS total_price;
```

```
DUMP total_price;
```

### 17. Tổng thể loại sách

```
type_of_books = GROUP books BY type_of_book;
```

```
DUMP type_of_books;
```

### 18. Thể loại có nhiều sách nhất

```
most_books = FOREACH type_of_books GENERATE group, COUNT(books) AS total_books;
most_books = ORDER most_books BY total_books DESC;
most_books = LIMIT most_books 1;
```

```
DUMP most_books;
```

### 19. Thể loại có ít sách nhất

```
least_books = FOREACH type_of_books GENERATE group, COUNT(books) AS total_books;
least_books = ORDER least_books BY total_books ASC;
least_books = LIMIT least_books 1;
```

```
DUMP least_books;
```

### 20. Tổng số sách theo thể loại

```
total_books_by_type = FOREACH type_of_books GENERATE group, COUNT(books) AS total_books;
```

```
DUMP total_books_by_type;
```
### 21. Lọc ra sách có giá bằng X hoặc Y
```
filter_books_XY = FILTER books BY (price == 51.77) or (price == 50.1);
```

```
DUMP filter_books_XY;
```


## Tham khảo thêm tại trang web:

- [Apache Pig Tutorial](https://www.tutorialspoint.com/apache_pig/)
