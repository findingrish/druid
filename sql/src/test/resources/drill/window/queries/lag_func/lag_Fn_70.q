SELECT col2 , LAG_col2 FROM ( SELECT col2 , LAG(col2) OVER( PARTITION BY col3 ORDER BY col1 asc ) LAG_col2 FROM "fewRowsAllData.parquet") sub_query WHERE LAG_col2 IN ('CA','CO','LA','OR','NH')