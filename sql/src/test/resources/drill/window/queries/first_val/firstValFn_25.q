SELECT col7 , col6 , FIRST_VALUE(col6) OVER(PARTITION BY col7 ORDER BY col6) FIRST_VALUE_col6 FROM "allTypsUniq.parquet" WHERE col6 IN ("1952-08-14" , "1981-03-14" , "1947-05-12" , "1995-10-09" , "1968-02-03" , "1943-02-02" , "1957-12-10" , "2015-01-01" , "2013-01-03" , "1989-02-02" , "2007-10-01" , "1999-01-03" )