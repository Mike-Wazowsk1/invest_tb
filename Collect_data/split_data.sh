bash find . -name "*_err*" -exec mv {} errors  \;
bash find . -name "*[^_err].parquet" -exec mv {} shares  \;
