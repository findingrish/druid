select col0 , lead(col0) over(partition by col7 order by col0 ) lead_col0 from "allTypsUniq.parquet" where col0 >= 0 and col0 <= 2147483647